#include <sys/time.h>
#include <iostream>
#include <memory>
#include "env_osl.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/file_system.h"

namespace rocksdb
{

	void OSLFile::PrintMetaData()
	{
	}

	Status OSLEnv::NewSequentialFile(const std::string &fname,
			std::unique_ptr<SequentialFile> *result,
			const EnvOptions &options)
	{

		if (IsFilePosix(fname) || files[fname] == NULL)
		{
			return posixEnv->NewSequentialFile(fname, result, options);
		}

		result->reset();

		OSLSequentialFile *f = new OSLSequentialFile(fname, this, options);
		result->reset(dynamic_cast<SequentialFile *>(f));

		return Status::OK();
	}

	Status OSLEnv::NewRandomAccessFile(const std::string &fname,
			std::unique_ptr<RandomAccessFile> *result,
			const EnvOptions &options)
	{

		if (IsFilePosix(fname))
		{
			return posixEnv->NewRandomAccessFile(fname, result, options);
		}

		OSLRandomAccessFile *f = new OSLRandomAccessFile(fname, this, options);
		result->reset(dynamic_cast<RandomAccessFile *>(f));

		return Status::OK();
	}

	Status OSLEnv::NewWritableFile(const std::string &fname,
			std::unique_ptr<WritableFile> *result,
			const EnvOptions &options)
	{

		uint32_t fileNum = 0;

		if (IsFilePosix(fname))
		{
			return posixEnv->NewWritableFile(fname, result, options);
		}
		else
		{
			posixEnv->NewWritableFile(fname, result, options);
		}

		fileNum = files.count(fname);

		if (fileNum != 0)
		{
			delete files[fname];
			files.erase(fname);
		}

		files[fname] = new OSLFile(fname);
		files[fname]->uuididx = uuididx++;

		OSLWritableFile *f = new OSLWritableFile(fname, this, options);
		result->reset(dynamic_cast<WritableFile *>(f));

		return Status::OK();
	}

	Status OSLEnv::DeleteFile(const std::string &fname)
	{
		if (IsFilePosix(fname))
		{
			return posixEnv->DeleteFile(fname);
		}
		posixEnv->DeleteFile(fname);

		if (files.find(fname) == files.end() || files[fname] == NULL)
		{
			return Status::OK();
		}

		OSLFile *oslfile = files[fname];

		for (auto it = oslfile->lbas.begin(); it != oslfile->lbas.end(); it++)
		{
			free_lbas.push(*it);
		}

		delete files[fname];
		files.erase(fname);
		return Status::OK();
	}

	Status OSLEnv::GetFileSize(const std::string &fname, std::uint64_t *size)
	{
		if (IsFilePosix(fname))
		{
			return posixEnv->GetFileSize(fname, size);
		}

		if (files.find(fname) == files.end() || files[fname] == NULL)
		{
			return Status::OK();
		}

		*size = files[fname]->size;

		return Status::OK();
	}

	Status OSLEnv::GetFileModificationTime(const std::string &fname,
			std::uint64_t *file_mtime)
	{

		if (IsFilePosix(fname))
		{
			return posixEnv->GetFileModificationTime(fname, file_mtime);
		}

		*file_mtime = 0;

		return Status::OK();
	}

	Status OSLEnv::RenameFile(const std::string &src,
			const std::string &target)
	{

		if (IsFilePosix(src))
		{
			return posixEnv->RenameFile(src, target);
		}

		posixEnv->RenameFile(src, target);

		if (files.find(src) == files.end() || files[src] == NULL)
		{
			return Status::OK();
		}

		OSLFile *osl = files[src];
		osl->name = target;
		files[target] = osl;
		files.erase(src);

		return Status::OK();
	}

	void OSLEnv::PrintMetaData()
	{
		std::map<std::string, OSLFile *>::iterator iter;
		for (iter = files.begin(); iter != files.end(); ++iter)
		{
			OSLFile *oslfile = iter->second;
			if (oslfile != NULL)
			{
				oslfile->PrintMetaData();
			}
		}
	}

	Status NewOSLEnv(Env **osl_env, const std::string &dev_name)
	{
		OSLEnv *oslEnv = new OSLEnv(dev_name);
		*osl_env = oslEnv;
		return Status::OK();
	}

} // namespace rocksdb
