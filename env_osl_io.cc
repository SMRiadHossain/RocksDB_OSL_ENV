#include <string.h>
#include <sys/time.h>

#include <atomic>
#include <iostream>

#include "env_osl.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

namespace rocksdb
{

	/* ### SequentialFile method implementation ### */

	Status OSLSequentialFile::ReadOffset(uint64_t offset, size_t n, Slice *result,
			char *scratch, size_t *readLen) const
	{
		size_t piece_off = 0, left, size;
		int ret;
		unsigned i;
		uint64_t off;

		if (oslfile == NULL || offset >= oslfile->size)
		{
			return Status::OK();
		}

		if (offset + n > oslfile->size)
		{
			n = oslfile->size - offset;
		}

		struct csd_params parameters;

		parameters.buffer1.command[0] = READ;
		parameters.data_pointer = scratch;

		for (auto it = oslfile->lbas.begin(); it != oslfile->lbas.end(); it++)
		{
			parameters.lba = *it;
			if (!syscall(__NR_csd_syscall, (void *)&parameters))
			{
				printf("Systemcall successfull\n");
			}
			else
			{
				printf("Error systemcall\n");
			}
			parameters.data_pointer = scratch + 4096;
		}

		*readLen = n;
		*result = Slice(scratch + offset, n);
		return Status::OK();
	}

	Status OSLSequentialFile::Read(size_t n, Slice *result, char *scratch)
	{
		size_t readLen = 0;
		Status status = ReadOffset(read_off, n, result, scratch, &readLen);

		if (status.ok())
			read_off += readLen;

		return status;
	}

	Status OSLSequentialFile::PositionedRead(uint64_t offset, size_t n,
			Slice *result, char *scratch)
	{
		size_t readLen = 0;
		Status status = ReadOffset(read_off, n, result, scratch, &readLen);

		*result = Slice(scratch, readLen);

		return Status::OK();
	}

	Status OSLSequentialFile::Skip(uint64_t n)
	{
		if (read_off + n <= oslfile->size)
		{
			read_off += n;
		}
		return Status::OK();
	}

	Status OSLSequentialFile::InvalidateCache(size_t offset, size_t length)
	{
		return Status::OK();
	}

	/* ### RandomAccessFile method implementation ### */

	Status OSLRandomAccessFile::ReadOffset(uint64_t offset, size_t n, Slice *result,
			char *scratch) const
	{
		struct zrocks_map *map;
		size_t piece_off = 0, left, msize;
		int ret;
		unsigned i;
		uint64_t off;

		if (oslfile == NULL || offset >= oslfile->size)
		{
			return Status::OK();
		}

		if (offset + n > oslfile->size)
		{
			n = oslfile->size - offset;
		}

		struct csd_params parameters;

		parameters.buffer1.command[0] = 'd';
		parameters.data_pointer = scratch;

		for (auto it = oslfile->lbas.begin(); it != oslfile->lbas.end(); it++)
		{
			parameters.lba = *it;
			if (!syscall(__NR_csd_syscall, (void *)&parameters))
			{
				printf("Systemcall successfull\n");
			}
			else
			{
				printf("Error systemcall\n");
			}
			parameters.data_pointer = scratch + 4096;
		}

		*result = Slice(scratch + offset, n);
		return Status::OK();
	}

	Status OSLRandomAccessFile::ReadObj(uint64_t offset, size_t n, Slice *result,
			char *scratch) const
	{
		return ReadOffset(offset, n, result, scratch);
	}

	Status OSLRandomAccessFile::Read(uint64_t offset, size_t n, Slice *result,
			char *scratch) const
	{
		return ReadOffset(offset, n, result, scratch);
	}

	Status OSLRandomAccessFile::Prefetch(uint64_t offset, size_t n)
	{

		if (env_osl->files[filename_] == NULL)
		{
			return Status::OK();
		}

		return Status::OK();
	}

	size_t OSLRandomAccessFile::GetUniqueId(char *id, size_t max_size) const
	{
		if (max_size < (kMaxVarint64Length * 3))
		{
			return 0;
		}

		char *rid = id;
		uint64_t base = 0;
		OSLFile *oslFilePtr = NULL;

		oslFilePtr = env_osl->files[filename_];

		if (!oslFilePtr)
		{
			std::cout << "the osl random file ptr is null"
				<< "file name is " << filename_ << std::endl;
			base = (uint64_t)this;
		}
		else
		{
			base = ((uint64_t)env_osl->files[filename_]->uuididx);
		}

		rid = EncodeVarint64(rid, (uint64_t)oslFilePtr);
		rid = EncodeVarint64(rid, base);
		assert(rid >= id);

		return static_cast<size_t>(rid - id);
	}

	Status OSLRandomAccessFile::InvalidateCache(size_t offset, size_t length)
	{
		return Status::OK();
	}

	/* ### WritableFile method implementation ### */
	Status OSLWritableFile::Append(const rocksdb::Slice &,
			const rocksdb::DataVerificationInfo &)
	{
		return Status::OK();
	}

	Status OSLWritableFile::PositionedAppend(const rocksdb::Slice &, uint64_t,
			const rocksdb::DataVerificationInfo &)
	{
		return Status::OK();
	}
	Status OSLWritableFile::Append(const Slice &data)
	{
		size_t size = data.size();
		size_t offset = 0;
		Status s;

		if (!cache_off)
		{
			std::cout << __func__ << filename_ << " failed : cache is NULL." << std::endl;
			return Status::IOError();
		}

		size_t cpytobuf_size = OSL_MAX_BUF;
		if (cache_off + data.size() > write_cache + OSL_MAX_BUF)
		{

			size = cpytobuf_size - (cache_off - write_cache);
			memcpy(cache_off, data.data(), size);
			cache_off += size;
			s = Sync();
			if (!s.ok())
			{
				return Status::IOError();
			}

			offset = size;
			size = data.size() - size;

			while (size > cpytobuf_size)
			{
				memcpy(cache_off, data.data() + offset, cpytobuf_size);
				offset = offset + cpytobuf_size;
				size = size - cpytobuf_size;
				cache_off += cpytobuf_size;
				s = Sync();
				if (!s.ok())
				{
					return Status::IOError();
				}
			}
		}

		memcpy(cache_off, data.data() + offset, size);
		cache_off += size;
		filesize_ += data.size();

		oslfile->size += data.size();

		return Status::OK();
	}

	Status OSLWritableFile::PositionedAppend(const Slice &data, uint64_t offset)
	{

		if (offset != filesize_)
		{
			std::cout << "Write Violation: " << __func__ << " size: " << data.size()
				<< " offset: " << offset << std::endl;
		}

		return Append(data);
	}

	Status OSLWritableFile::Truncate(uint64_t size)
	{
		if (env_osl->files[filename_] == NULL)
		{
			return Status::OK();
		}

		size_t cache_size = (size_t)(cache_off - write_cache);
		size_t trun_size = oslfile->size - size;

		if (cache_size < trun_size)
		{
			return Status::OK();
		}

		cache_off -= trun_size;
		filesize_ = size;
		oslfile->size = size;

		return Status::OK();
	}

	Status OSLWritableFile::Close()
	{
		Sync();
		return Status::OK();
	}

	Status OSLWritableFile::Flush()
	{
		return Status::OK();
	}

	Status OSLWritableFile::Sync()
	{

		uint16_t pieces = 0;
		size_t size;
		int ret, i;

		if (!cache_off)
			return Status::OK();

		size = (size_t)(cache_off - write_cache);
		if (!size)
			return Status::OK();

		struct csd_params parameters;
		parameters.buffer1.command[0] = WRITE;

		for (uint32_t id = 0; id < size, id += 4096)
		{
			lbas.push_back(oslfile->free_lbas.top());
			parameters.lba = oslfile->free_lbas.top();
			parameters.data_pointer = write_cache + id;
			if (!syscall(__NR_csd_syscall, (void *)&parameters))
			{
				printf("Systemcall successfull\n");
			}
			else
			{
				printf("Error systemcall\n");
				std::cout << __func__ << " file: " << filename_
					<< " ZRocks (write) error: " << ret << std::endl;
				return Status::IOError();
			}
			oslfile->free_lbas.pop();
		}


		cache_off = write_cache;
		return Status::OK();
	}

	Status OSLWritableFile::Fsync()
	{
		return Sync();
	}

	Status OSLWritableFile::InvalidateCache(size_t offset, size_t length)
	{
		return Status::OK();
	}

	void OSLWritableFile::SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint)
	{
		write_hint_ = hint;
		oslfile->level = hint - Env::WLTH_SHORT;
	}

	Status OSLWritableFile::RangeSync(uint64_t offset, uint64_t nbytes)
	{
		return Status::OK();
	}

	size_t OSLWritableFile::GetUniqueId(char *id, size_t max_size) const
	{
		if (max_size < (kMaxVarint64Length * 3))
		{
			return 0;
		}

		char *rid = id;
		uint64_t base = 0;
		if (!env_osl->files[filename_])
		{
			base = (uint64_t)this;
		}
		else
		{
			base = ((uint64_t)oslfile->uuididx);
		}
		rid = EncodeVarint64(rid, (uint64_t)base);
		rid = EncodeVarint64(rid, (uint64_t)base);
		rid = EncodeVarint64(rid, (uint64_t)base);
		assert(rid >= id);

		return static_cast<size_t>(rid - id);
	}

} // namespace rocksdb
