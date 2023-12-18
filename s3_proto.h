#ifdef __cplusplus

#include <map>
#include <vector>
#include <string>
#include <memory>
#include <list>
#include <condition_variable>

namespace Aws {
  namespace S3 {
    namespace Model {
      class CompletedPart;
    }
  }
}

class s3_proto : public std::enable_shared_from_this<s3_proto>
{
  struct block {
    std::condition_variable cond;
    bool read_complete{false};
    bool read_succeeded{false};
    std::string data;
    uint64_t block_offset;

    block(uint64_t block_offset, size_t data_size)
      : block_offset(block_offset)
    {
      data.resize(data_size);
    }
  };

  std::string bucket;
  std::string key;
  uint64_t file_size{0};
  const int k_block_size{1024*1024*8};
  const int k_max_cached_blocks{6};
  const int k_close_to_edge{1024*1024*1};
  std::mutex mtx;
  std::map<uint64_t, std::shared_ptr<block>> blocks;
  uint64_t file_pos{0};
  std::list<block*> last_accessed_blocks;
  bool is_read{false};
  std::string multipart_upload_id;
  int outstanding_writes{0};
  bool async_write_failure{false};
  std::map<int, Aws::S3::Model::CompletedPart> completed_parts;
  std::condition_variable cond;

  void issue_read(uint64_t block_start, const std::unique_lock<std::mutex>&);
  void flush_completed_blocks(const std::unique_lock<std::mutex>&);
  void write_one_block(const std::shared_ptr<block>& b);
  std::shared_ptr<block> find_block(uint64_t pos, const std::unique_lock<std::mutex>&);
  void abort_multipart_upload();

public:
  s3_proto(const std::string& url, int access);
  ~s3_proto();
  int read(void* buf, int sz);
  int write(const void* buf, int sz);
  int64_t seek(int64_t pos, int whence);
  int finialize_write();

  const std::string& get_bucket() const;
  const std::string& get_key() const;
  bool get_is_read() const;
  int64_t get_size() const;
};

#else

typedef struct _s3_proto s3_proto;

#endif

#ifdef __cplusplus
extern "C" {
#endif

s3_proto* s3_proto_new(const char* url, int access);
int s3_proto_read(s3_proto* p, void* buf, int sz);
int s3_proto_write(s3_proto* p, const void* buf, int sz);
int64_t s3_proto_seek(s3_proto* p, int64_t pos, int whence); 
int64_t s3_proto_size(s3_proto* p);
int s3_proto_close_and_delete(s3_proto* p);

#ifdef __cplusplus
}
#endif