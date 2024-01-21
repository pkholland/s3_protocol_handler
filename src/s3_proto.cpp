#include "s3_proto.h"
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/client/RetryStrategy.h>
#include <fcntl.h>

// copied fromm ffmpeg sources
#define MKTAG(a,b,c,d)   ((a) | ((b) << 8) | ((c) << 16) | ((unsigned)(d) << 24))
#define FFERRTAG(a, b, c, d) (-(int)MKTAG(a, b, c, d))
#define AVERROR_EOF                FFERRTAG( 'E','O','F',' ') ///< End of file


using namespace Aws::S3::Model;

namespace
{

  std::mutex global_mtx;
  std::map<s3_proto*, std::shared_ptr<s3_proto>> instances;
  Aws::SDKOptions options;
  std::shared_ptr<Aws::S3::S3Client> global_s3_client;
  std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor> global_thread_pool;

  std::shared_ptr<Aws::S3::S3Client> regional_s3_client(const std::string& region)
  {
    Aws::S3::S3ClientConfiguration  config;
    config.executor = global_thread_pool;
    config.region = region;
    return std::make_shared<Aws::S3::S3Client>(config);
  }

  // class that makes sure we have a valid global_s3_client
  // whenever there are one or more instances of s3_proto
  class aws_initer
  {
  public:
    aws_initer()
    {
      if (instances.empty())
      {
        //options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
        Aws::InitAPI(options);
        global_thread_pool = std::make_shared<Aws::Utils::Threading::PooledThreadExecutor>(6);
        Aws::S3::S3ClientConfiguration  config;
        config.executor = global_thread_pool;
        //config.retryStrategy = std::make_shared<Aws::Client::StandardRetryStrategy>(10);
        global_s3_client = std::make_shared<Aws::S3::S3Client>(config);
      }
    }

    ~aws_initer()
    {
      if (instances.empty())
      {
        global_s3_client.reset();
        global_thread_pool.reset();
        Aws::ShutdownAPI(options);
      }
    }
  };

}

s3_proto::s3_proto(const std::string &url, int access)
  : s3_client(global_s3_client)
{
  if (url.find("s3://") != 0) {
    throw std::runtime_error("invalid url");
  }
  auto path = url.substr(5); // skip "s3://"
  auto first_slash_pos = path.find("/");
  if (first_slash_pos == std::string::npos) {
    throw std::runtime_error("invalid url");
  }
  bucket = path.substr(0, first_slash_pos);
  key = path.substr(first_slash_pos + 1);
  if (key.empty()) {
    throw std::runtime_error("invalid url");
  }

  // while the sdk supports behaviors to automatically redirect
  // s3 read/write to the correct region, it does so by following
  // http redirects on each call.  Since we will be making multiple
  // calls to these buckets we would rather find the right region
  // once and reset our client, if necessary, to the region corresponding
  // to the bucket.
  // https://github.com/aws/aws-sdk-go/issues/720 has a discussion with
  // comments from aws engineers claiming that "HeadBucket" is the ideal
  // call to make in this kind of usage.  Note that usually when the
  // given s3 url is for a bucket outside of the region where this code
  // is running in, s3 returns a 301 response, so we take the first
  // branch and find the right region in the headers that are returned.
  HeadBucketRequest hbReq;
  hbReq.WithBucket(bucket);
  auto reply = s3_client->HeadBucket(hbReq);
  if (!reply.IsSuccess()) {
    auto &err = reply.GetError();
    std::cerr << "HeadBucket(" << bucket << ") failed\n" << err << "\n";
    if ((err.GetResponseCode() == Aws::Http::HttpResponseCode::MOVED_PERMANENTLY)
      || (err.GetResponseCode() == Aws::Http::HttpResponseCode::FORBIDDEN)) {
      auto &headers = err.GetResponseHeaders();
      auto it = headers.find("x-amz-bucket-region");
      if (it != headers.end()) {
        std::cerr << "reset s3_client to " << it->second << " via MOVED_PERMANENTLY error\n";
        s3_client = regional_s3_client(it->second);
      }
    }
  }
  else {
    auto &result = reply.GetResult();
    auto region = result.GetBucketRegion();
    std::cerr << "reset s3_client to " << region << " via HeadBucket succeeding\n";
    s3_client = regional_s3_client(region);
  }

  if (!(access & O_CREAT)) {
    if (access != O_RDONLY) {
      throw std::runtime_error("invalid access");
    }
    is_read = true;
    HeadObjectRequest req;
    req.WithBucket(bucket)
        .WithKey(key);
    auto reply = s3_client->HeadObject(req);
    if (!reply.IsSuccess()) {
      throw std::runtime_error("unable to read file");
    }
    auto result = reply.GetResult();
    file_size = result.GetContentLength();
  }
  else {
    if (access != (O_WRONLY | O_CREAT)) {
      throw std::runtime_error("invalid access");
    }
  }
}

s3_proto::~s3_proto()
{
}

// find the block in our cache that contains the byte for file offset 'pos'
std::shared_ptr<s3_proto::block> s3_proto::find_block(uint64_t pos, const std::unique_lock<std::mutex>&)
{
  if (!blocks.empty()) {
    auto block_it = blocks.lower_bound(pos);
    if (block_it != blocks.end() && block_it->first == pos) {
      return block_it->second;
    }
    if (block_it != blocks.begin()) {
      auto prev_block_it = std::prev(block_it);
      auto end_pos = prev_block_it->first + prev_block_it->second->data.size();
      if (end_pos > pos) {
        return prev_block_it->second;
      }
    }
  }
  return {};
}

// issue an async read request for the byte range starting at 'block_start'
void s3_proto::issue_read(uint64_t block_start, const std::unique_lock<std::mutex>&)
{
  // if we already have the maximum number of blocks cached
  // release the oldest one.
  while (last_accessed_blocks.size() >= k_max_cached_read_blocks) {
    auto last = std::prev(last_accessed_blocks.end());
    if (!(*last)->read_complete) {
      break;
    }
    auto b_it = blocks.find((*last)->block_offset);
    if (b_it != blocks.end()) {
      blocks.erase(b_it);
    }
    last_accessed_blocks.erase(last);
  }

  // allocate the memory for the block we are going to read
  // and link it to the lists we use to track things.
  auto block_end = block_start + k_read_block_size;
  if (block_end > file_size) {
    block_end = file_size;
  }
  auto b = std::make_shared<block>(block_start, block_end - block_start);
  blocks.emplace(block_start, b);
  last_accessed_blocks.push_front(b.get());

  // the aws c++ sdk call to read a byte range from an s3 object
  // this API requires a callback function arg which will get called
  // when the s3 REST call completes.
  std::ostringstream range;
  range << "bytes=" << b->block_offset << "-" << b->block_offset + b->data.size() - 1;
  GetObjectRequest req;
  req.WithBucket(get_bucket())
    .WithKey(get_key())
    .WithRange(range.str());
  s3_client->GetObjectAsync(req,
    [b,ths = shared_from_this()]
    (auto client, auto &req, auto outcome, auto ctx) {
      // boilerplate code for getting answers
      // back from the aws sdk
      if (outcome.IsSuccess()) {
        auto &result = outcome.GetResult();
        if (result.GetContentLength() == b->data.size()) { // TODO: maybe make this more forgiving??
          auto &body = result.GetBody();
          body.read(&b->data[0], b->data.size());
          b->read_succeeded = body.good();
        }
        else {
          b->read_succeeded = false;
        }
      }
      else {
        b->read_succeeded = false;
      }
      std::lock_guard<std::mutex> l(ths->mtx);
      b->read_complete = true;
      b->cond.notify_all();
    });
}

// upload one multipart block
void s3_proto::write_one_block(const std::shared_ptr<block>& b)
{
  auto part_num = 1 + (b->block_offset / k_write_block_size);
  auto &data = b->data;
  UploadPartRequest req;
  req.WithBucket(bucket)
    .WithKey(key)
    .WithUploadId(multipart_upload_id)
    .WithPartNumber(part_num);
  auto strm = std::make_shared<Aws::StringStream>(std::move(data));
  req.SetBody(strm);
  ++outstanding_writes;
  s3_client->UploadPartAsync(req, [part_num, ths = shared_from_this()]
  (auto client, auto &req, auto &outcome, auto ctx) {
    std::lock_guard<std::mutex> l(ths->mtx);
    if (outcome.IsSuccess()) {
      auto &result = outcome.GetResult();
      ths->completed_parts[part_num] = CompletedPart()
        .WithPartNumber(part_num)
        .WithETag(result.GetETag());
    }
    else {
      ths->async_write_failure = true;
    }
    if (--ths->outstanding_writes == 0) {
      ths->cond.notify_all();
    }
  });

}

// upload and release blocks that are far enough
// behind file_pos
void s3_proto::flush_completed_blocks(const std::unique_lock<std::mutex>&)
{
  if (file_size < k_write_block_size) {
    return;
  }

  // if file_pos gets >= k_block_size then we are going to
  // use multipart upload.  If we haven't yet started that
  // do that now.
  if (multipart_upload_id.empty()) {
    CreateMultipartUploadRequest req;
    req.WithBucket(bucket)
      .WithKey(key);
    auto outcome = s3_client->CreateMultipartUpload(req);
    if (!outcome.IsSuccess()) {
      throw std::runtime_error("CreateMultipartUpload failed");
    }
    auto &result = outcome.GetResult();
    multipart_upload_id = result.GetUploadId();
  }

  // write to s3 any block that is not the
  // first block, and whose end position is
  // more than 16MB prior to file_pos.
  for (auto it = blocks.begin(); it != blocks.end();) {
    auto b = it->second;
    if (b->block_offset > 0 && file_pos > b->block_offset + b->data.size() + 1024*1024*16) {
      write_one_block(b);
      it = blocks.erase(it);
    }
    else {
      it++;
    }
  }
}

// clean up some of the S3 resources if
// we fail half way through writing
void s3_proto::abort_multipart_upload()
{
  AbortMultipartUploadRequest req;
  req.WithBucket(bucket)
    .WithKey(key)
    .WithUploadId(multipart_upload_id);
  s3_client->AbortMultipartUpload(req);
}

// called at the end of writing.  It finishes up
// the upload dealing with both single and multipart
int s3_proto::finialize_write()
{
  if (multipart_upload_id.empty()) {
    // in this case there was never a write that caused the
    // logical file size to exceed k_block_size, which means
    // all of the written data is in the first block, and
    // it is small enough to write to S3 in a single upload.
    PutObjectRequest req;
    req.WithBucket(bucket)
      .WithKey(key);

    std::shared_ptr<Aws::StringStream> strm;
    if (file_size > 0) {
      auto it = blocks.begin();
      auto &data = it->second->data;
      strm = std::make_shared<Aws::StringStream>(std::string(&data[0], file_size));
    }
    else {
      strm = std::make_shared<Aws::StringStream>("");
    }
    req.SetBody(strm);
    return s3_client->PutObject(req).IsSuccess() ? 0 : -(EACCES);
  }
  else {
    if (async_write_failure) {
      abort_multipart_upload();
      return -(EACCES);
    }
    // we are going to upload with multiple part upload.

    // the last block likely has less data written to
    // it than the buffer size.  So resize it so that
    // it correctly represents file_size
    auto last = std::prev(blocks.end());
    auto last_offset = last->second->block_offset;
    last->second->data.resize(file_size - last_offset);

    // write all blocks that we still have in memory
    for (auto b : blocks) {
      write_one_block(b.second);
    }
    blocks.clear();

    // wait for those block writes to complete
    std::unique_lock<std::mutex> l(mtx);
    while (outstanding_writes > 0) {
      cond.wait(l);
    }

    // now, even though this is unlikely, it is possible (and legal)
    // for the caller to do something like open a file, seek to 100000000
    // and then write a byte and close.  That would cause there to
    // be blocks that we never allocated or wrote, and they should
    // be set to all zeros.  So walk the list of completed_parts
    // and issue write calls for any holes we find.
    auto part_num = 1;
    auto wrote_some = false;
    for (auto cp : completed_parts) {
      while (cp.first > part_num) {
        wrote_some = true;
        write_one_block(std::make_shared<block>((part_num-1)*k_write_block_size, k_write_block_size));
        ++part_num;
      }
      ++part_num;
    }

    if (wrote_some) {
      while (outstanding_writes > 0) {
        cond.wait(l);
      }
    }

    if (async_write_failure) {
      abort_multipart_upload();
      return -(EACCES);
    }

    Aws::Vector<CompletedPart> parts(completed_parts.size());
    part_num = 0;
    for (auto cp : completed_parts) {
      parts[part_num++] = cp.second;
    }

    CompleteMultipartUploadRequest cmpu_req;
    cmpu_req.WithBucket(bucket)
      .WithKey(key)
      .WithUploadId(multipart_upload_id)
      .WithMultipartUpload(CompletedMultipartUpload().WithParts(std::move(parts)));
    return s3_client->CompleteMultipartUpload(cmpu_req).IsSuccess() ? 0 : -(EACCES);
  }
}

int s3_proto::read(void *buf, int sz)
{
  std::unique_lock<std::mutex> l(mtx);
  if (auto block_ptr = find_block(file_pos, l)) {
    auto block_start = block_ptr->block_offset;
    auto block_end = block_start + block_ptr->data.size();

    // here block_ptr includes at least one byte starting at 'file_pos',
    // but because we read asynchronously it is possible that we
    // are still waiting for a read to complete on this block.
    while (!block_ptr->read_complete) {
      block_ptr->cond.wait(l);
    }
    // remember that reads can fail...
    if (!block_ptr->read_succeeded) {
      return -1;
    }

    auto copy_sz = block_end - file_pos;
    if (copy_sz > sz) {
      copy_sz = sz;
    }
    memcpy(buf, &block_ptr->data[file_pos - block_start], copy_sz);
    auto orig_file_pos = file_pos;
    file_pos += copy_sz;

    block* block_to_move{nullptr};
    if (copy_sz < sz) {
      // in all likelihood, we will receive another read
      // for the rest of what the caller wants.  If that
      // block is already available, then move that one to
      // the front
      auto it = blocks.find(file_pos);
      if (it != blocks.end()) {
        block_to_move = it->second.get();
      }
    }
    else {
      block_to_move = block_ptr.get();
    }

    // record the fact that we read from this block by moving
    // the 'block_to_move' item in last_accessed_blocks to the
    // front of that list.
    if (block_to_move) {
      auto it = std::find(last_accessed_blocks.begin(), last_accessed_blocks.end(), block_to_move);
      if (it != last_accessed_blocks.begin()) {
        if (it != last_accessed_blocks.end()) {
          last_accessed_blocks.erase(it);
        }
        last_accessed_blocks.push_front(block_to_move);
      }
    }

    // if the updated file_pos is close enough to the end of this
    // block (or is already beyond this block), and this block
    // is not the last block of the file, then check to see if
    // have already issued a read for the next block.  If not,
    // issue one now.
    if (block_end < file_size && (block_end - file_pos < k_close_to_edge)) {
      auto next_it = blocks.find(block_end);
      if (next_it == blocks.end()) {
        issue_read(block_end, l);
      }
    }

    // similarly, issue prefetch reads for the block before this one
    // note that as long as the file access pattern is largely
    // either progressing forward or backward through the file,
    // then these two prefetch calls tend to only acutally prefetch
    // one of the two.
    if (block_start > 0 && (orig_file_pos - block_start < k_close_to_edge)) {
      auto prev_it= blocks.find(block_start - k_read_block_size);
      if (prev_it == blocks.end()) {
        issue_read(block_start - k_read_block_size, l);
      } 
    }

    return copy_sz;
  }

  if (file_pos >= file_size) {
    return AVERROR_EOF;
  }

  // here we don't already have a cached block for this byte, so start
  // a new fetch for that block.
  auto block_start = file_pos / k_read_block_size * k_read_block_size;
  issue_read(block_start, l);
  l.unlock();
  return read(buf, sz);
}

int s3_proto::write(const void *buf, int sz)
{
  std::unique_lock<std::mutex> l(mtx);
  if (async_write_failure) {
    return -(EACCES);
  }
  if (auto block_ptr = find_block(file_pos, l)) {
    auto block_start = block_ptr->block_offset;
    auto block_end = block_start + block_ptr->data.size();

    auto copy_sz = block_end - file_pos;
    if (copy_sz > sz) {
      copy_sz = sz;
    }
    memcpy(&block_ptr->data[file_pos - block_start], buf, copy_sz);
    file_pos += copy_sz;
    file_size = std::max(file_pos, file_size);
    flush_completed_blocks(l);
    return (int)copy_sz;
  }

  auto block_start = file_pos / k_write_block_size * k_write_block_size;
  auto b = std::make_shared<block>(block_start, k_write_block_size);
  blocks.emplace(block_start, b);
  l.unlock();
  return write(buf, sz);
}

int64_t s3_proto::seek(int64_t pos, int whence)
{
  switch (whence) {
    case SEEK_SET:
      file_pos = pos;
      break;
    case SEEK_CUR:
      file_pos += pos;
      break;
    case SEEK_END:
      file_pos = file_size + pos;
      break;
  }
  if (file_pos < 0) {
    file_pos = 0;
  }
  else if (file_pos > file_size) {
    file_pos = file_size;
  }
  return file_pos;
}

int64_t s3_proto::get_size() const
{
  return file_size;
}

const std::string& s3_proto::get_bucket() const
{
  return bucket;
}

const std::string& s3_proto::get_key() const
{
  return key;
}

bool s3_proto::get_is_read() const
{
  return is_read;
}

s3_proto *s3_proto_new(const char *url, int access)
{
  try
  {
    std::lock_guard<std::mutex> l(global_mtx);
    aws_initer init;
    auto sp = std::make_shared<s3_proto>(url, access);
    auto p = sp.get();
    instances.emplace(p, sp);
    return p;
  }
  catch (...)
  {}
  return {};
}

int s3_proto_read(s3_proto * p, void *buf, int sz)
{
  try
  {
    if (!p->get_is_read()) {
      return -1;
    }
    return p->read(buf, sz);
  }
  catch (...)
  {}
  return -(ENOENT);
}

int s3_proto_write(s3_proto * p, const void *buf, int sz)
{
  try {
    if (p->get_is_read()) {
      return -1;
    }
    return p->write(buf, sz);
  }
  catch(...)
  {}
  return -(EACCES);
}

int64_t s3_proto_seek(s3_proto * p, int64_t pos, int whence)
{
  return p->seek(pos, whence);
}

int64_t s3_proto_size(s3_proto *p)
{
  return p->get_size();
}

int s3_proto_close_and_delete(s3_proto * p)
{
  auto ret = 0;
  if (!p->get_is_read()) {
    ret = p->finialize_write();
  }
  std::lock_guard<std::mutex> l(global_mtx);
  aws_initer init;
  auto it = instances.find(p);
  if (it != instances.end()) {
    instances.erase(it);
  }
  return ret;
}
