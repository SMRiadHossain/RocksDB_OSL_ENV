#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <queue>

#include "rocksdb/env.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/object_registry.h"

#define OSL_ALIGMENT 4096
#define OSL_MAX_BUF (OSL_ALIGMENT * 65536)

#define GET_NANOSECONDS(ns, ts)                       \
	do                                                  \
{                                                   \
	clock_gettime(CLOCK_REALTIME, &ts);               \
	(ns) = ((ts).tv_sec * 1000000000 + (ts).tv_nsec); \
} while (0)

#define MAX_KV_KEY_SIZE		16
#define __NR_csd_syscall	294

enum opcode{
	GETOBJECT = 'c',
	PUTOBJECT = 'd'
};


struct buff{
	char command[4];
};

struct csd_params {

	int ObjectID;
	int lba;
	char* data_pointer;
	struct buff buffer1;
};

namespace rocksdb
{

	/* ### OSL Environment ### */

	class OSLFile
	{
		public:
			std::string name;
			size_t size;
			size_t before_truncate_size;
			std::uint64_t uuididx;
			std::uint32_t startIndex;
			std::vector<uint32_t> lbas;

			OSLFile(const std::string &fname)
				: name(fname), uuididx(0)
			{
				before_truncate_size = 0;
				size = 0;
				startIndex = 0;
			}

			~OSLFile()
			{
			}

			void PrintMetaData();
	};

	class OSLEnv : public Env
	{
		public:
			std::map<std::string, OSLFile *> files;
			std::priority_queue<uint32_t> free_lbas;
			uint64_t sequence;

			std::uint64_t uuididx;

			explicit OSLEnv(const std::string &dname) : dev_name(dname)
			{
				posixEnv = Env::Default();
				uuididx = 0;
				sequence = 0;
				for(int i=0; i<100; i++) {
					free_lbas.push(i);
				}
				std::cout << "Initializing OSL Environment" << std::endl;
			}

			virtual ~OSLEnv()
			{
				std::cout << "Destroying OSL Environment" << std::endl;
			}

			void PrintMetaData();

			/* ### Implemented at env_osl.cc ### */

			Status NewSequentialFile(const std::string &fname,
					std::unique_ptr<SequentialFile> *result,
					const EnvOptions &options) override;

			Status NewRandomAccessFile(const std::string &fname,
					std::unique_ptr<RandomAccessFile> *result,
					const EnvOptions &options) override;

			Status NewWritableFile(const std::string &fname,
					std::unique_ptr<WritableFile> *result,
					const EnvOptions &options) override;

			Status DeleteFile(const std::string &fname) override;

			Status GetFileSize(const std::string &fname, std::uint64_t *size) override;

			Status GetFileModificationTime(const std::string &fname,
					std::uint64_t *file_mtime) override;

			Status RenameFile(const std::string &src,
					const std::string &target) override;

			/* ### Implemented here ### */

			Status LinkFile(const std::string & /*src*/,
					const std::string & /*target*/) override
			{
				return Status::NotSupported(); // not supported
			}

			static std::uint64_t gettid()
			{
				return 0;
			}

			std::uint64_t GetThreadID() const override
			{
				return OSLEnv::gettid();
			}

			/* ### Posix inherited functions ### */

			Status NewDirectory(const std::string &name,
					std::unique_ptr<Directory> *result) override
			{
				return posixEnv->NewDirectory(name, result);
			}

			Status FileExists(const std::string &fname) override
			{
				return posixEnv->FileExists(fname);
			}

			Status GetChildren(const std::string &path,
					std::vector<std::string> *result) override
			{
				return posixEnv->GetChildren(path, result);
			}

			Status CreateDir(const std::string &name) override
			{
				return posixEnv->CreateDir(name);
			}

			Status CreateDirIfMissing(const std::string &name) override
			{
				return posixEnv->CreateDirIfMissing(name);
			}

			Status DeleteDir(const std::string &name) override
			{
				return posixEnv->DeleteDir(name);
			}

			Status LockFile(const std::string &fname, FileLock **lock) override
			{
				return posixEnv->LockFile(fname, lock);
			}

			Status UnlockFile(FileLock *lock) override
			{
				return posixEnv->UnlockFile(lock);
			}

			Status IsDirectory(const std::string &path, bool *is_dir) override
			{
				return posixEnv->IsDirectory(path, is_dir);
			}

			Status NewLogger(const std::string &fname,
					std::shared_ptr<Logger> *result) override
			{
				return posixEnv->NewLogger(fname, result);
			}

			void Schedule(void (*function)(void *arg), void *arg, Priority pri = LOW,
					void *tag = nullptr,
					void (*unschedFunction)(void *arg) = 0) override
			{
				posixEnv->Schedule(function, arg, pri, tag, unschedFunction);
			}

			int UnSchedule(void *tag, Priority pri) override
			{
				return posixEnv->UnSchedule(tag, pri);
			}

			void StartThread(void (*function)(void *arg), void *arg) override
			{
				posixEnv->StartThread(function, arg);
			}

			void WaitForJoin() override
			{
				posixEnv->WaitForJoin();
			}

			unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const override
			{
				return posixEnv->GetThreadPoolQueueLen(pri);
			}

			Status GetTestDirectory(std::string *path) override
			{
				return posixEnv->GetTestDirectory(path);
			}

			std::uint64_t NowMicros() override
			{
				return posixEnv->NowMicros();
			}

			void SleepForMicroseconds(int micros) override
			{
				posixEnv->SleepForMicroseconds(micros);
			}

			Status GetHostName(char *name, std::uint64_t len) override
			{
				return posixEnv->GetHostName(name, len);
			}

			Status GetCurrentTime(int64_t *unix_time) override
			{
				return posixEnv->GetCurrentTime(unix_time);
			}

			Status GetAbsolutePath(const std::string &db_path,
					std::string *output_path) override
			{
				return posixEnv->GetAbsolutePath(db_path, output_path);
			}

			void SetBackgroundThreads(int number, Priority pri = LOW) override
			{
				posixEnv->SetBackgroundThreads(number, pri);
			}

			int GetBackgroundThreads(Priority pri = LOW) override
			{
				return posixEnv->GetBackgroundThreads(pri);
			}

			void IncBackgroundThreadsIfNeeded(int number, Priority pri) override
			{
				posixEnv->IncBackgroundThreadsIfNeeded(number, pri);
			}

			std::string TimeToString(std::uint64_t number) override
			{
				return posixEnv->TimeToString(number);
			}

			Status GetThreadList(std::vector<ThreadStatus> *thread_list)
			{
				return posixEnv->GetThreadList(thread_list);
			}

			ThreadStatusUpdater *GetThreadStatusUpdater() const override
			{
				return posixEnv->GetThreadStatusUpdater();
			}

		private:
			Env *posixEnv;
			const std::string dev_name;
			bool IsFilePosix(const std::string &fname)
			{
				return (fname.find("uuid") != std::string::npos ||
						fname.find("CURRENT") != std::string::npos ||
						fname.find("IDENTITY") != std::string::npos ||
						fname.find("MANIFEST") != std::string::npos ||
						fname.find("OPTIONS") != std::string::npos ||
						fname.find("LOG") != std::string::npos ||
						fname.find("LOCK") != std::string::npos ||
						fname.find(".dbtmp") != std::string::npos ||
						fname.find(".trace") != std::string::npos);
			}
	};

	/* ### SequentialFile, RandAccessFile, and Writable File ### */

	class OSLSequentialFile : public SequentialFile
	{
		private:
			std::string filename_;
			bool use_direct_io_;
			size_t logical_sector_size_;
			OSLFile *oslfile;
			OSLEnv *env_osl;
			uint64_t read_off;

		public:
			OSLSequentialFile(const std::string &fname, OSLEnv *osl,
					const EnvOptions &options)
				: filename_(fname),
				use_direct_io_(options.use_direct_reads),
				logical_sector_size_(OSL_ALIGMENT)
				{
					env_osl = osl;
					read_off = 0;
					oslfile = env_osl->files[fname];
				}

			virtual ~OSLSequentialFile()
			{
			}

			/* ### Implemented at env_osl_io.cc ### */

			Status ReadOffset(uint64_t offset, size_t n, Slice *result, char *scratch,
					size_t *readLen) const;

			Status Read(size_t n, Slice *result, char *scratch) override;

			Status PositionedRead(std::uint64_t offset, size_t n, Slice *result,
					char *scratch) override;

			Status Skip(std::uint64_t n) override;

			Status InvalidateCache(size_t offset, size_t length) override;

			/* ### Implemented here ### */

			bool use_direct_io() const override
			{
				return use_direct_io_;
			}

			size_t GetRequiredBufferAlignment() const override
			{
				return logical_sector_size_;
			}
	};

	class OSLRandomAccessFile : public RandomAccessFile
	{
		private:
			std::string filename_;
			bool use_direct_io_;
			size_t logical_sector_size_;
			std::uint64_t uuididx;

			OSLEnv *env_osl;
			OSLFile *oslfile;

		public:
			OSLRandomAccessFile(const std::string &fname, OSLEnv *osl,
					const EnvOptions &options)
				: filename_(fname),
				use_direct_io_(options.use_direct_reads),
				logical_sector_size_(OSL_ALIGMENT),
				uuididx(0),
				env_osl(osl)
		{

			oslfile = env_osl->files[filename_];

			virtual ~OSLRandomAccessFile()
			{
			}

			/* ### Implemented at env_osl_io.cc ### */

			Status Read(std::uint64_t offset, size_t n, Slice * result,
					char *scratch) const override;

			Status Prefetch(std::uint64_t offset, size_t n) override;

			size_t GetUniqueId(char *id, size_t max_size) const override;

			Status InvalidateCache(size_t offset, size_t length) override;

			virtual Status ReadObj(std::uint64_t offset, size_t n, Slice * result,
					char *scratch) const;

			virtual Status ReadOffset(std::uint64_t offset, size_t n, Slice * result,
					char *scratch) const;

			/* ### Implemented here ### */

			bool use_direct_io() const override
			{
				return use_direct_io_;
			}

			size_t GetRequiredBufferAlignment() const override
			{
				return logical_sector_size_;
			}
		};

		class OSLWritableFile : public WritableFile
		{
			private:
				const std::string filename_;
				const bool use_direct_io_;
				int fd_;
				std::uint64_t filesize_;
				OSLFile *oslfile;
				size_t logical_sector_size_;

				char *write_cache;
				char *cache_off;

				OSLEnv *env_osl;
				std::uint64_t map_off;

			public:
				explicit OSLWritableFile(const std::string &fname, OSLEnv *osl,
						const EnvOptions &options)
					: WritableFile(options),
					filename_(fname),
					use_direct_io_(options.use_direct_writes),
					fd_(0),
					filesize_(0),
					logical_sector_size_(OSL_ALIGMENT),
					env_osl(osl)
					{

						write_cache = (char *)malloc(OSL_MAX_BUF);
						if (!write_cache)
						{
							std::cout << " malloc error." << std::endl;
							cache_off = nullptr;
						}

						cache_off = write_cache;
						map_off = 0;

						oslfile = env_osl->files[fname];
					}

				virtual ~OSLWritableFile()
				{
					if (write_cache)
						free(write_cache);
				}

				/* ### Implemented at env_osl_io.cc ### */

				Status Append(const Slice &data) override;

				Status PositionedAppend(const Slice &data, std::uint64_t offset) override;
				Status Append(const rocksdb::Slice &, const rocksdb::DataVerificationInfo &);
				Status PositionedAppend(const rocksdb::Slice &, uint64_t,
						const rocksdb::DataVerificationInfo &);

				Status Truncate(std::uint64_t size) override;

				Status Close() override;

				Status Flush() override;

				Status Sync() override;

				Status Fsync() override;

				Status InvalidateCache(size_t offset, size_t length) override;

				void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override;

				Status RangeSync(std::uint64_t offset, std::uint64_t nbytes) override;

				size_t GetUniqueId(char *id, size_t max_size) const override;

				/* ### Implemented here ### */

				bool IsSyncThreadSafe() const override
				{
					return true;
				}

				bool use_direct_io() const override
				{
					return use_direct_io_;
				}

				size_t GetRequiredBufferAlignment() const override
				{
					return logical_sector_size_;
				}
		};

			Status NewOSLEnv(Env **osl_env, const std::string &dev_name);

	} // namespace rocksdb
