import tempfile
import shutil
import os
import pytest
from das.distributed_atom_space import DistributedAtomSpace

def test_get_file_list():

    tmp_prefix = 'das_pytest_'
    temp_dir1 = tempfile.mkdtemp(prefix=tmp_prefix)
    _, temp_file1 = tempfile.mkstemp(suffix=".metta", prefix=tmp_prefix)
    _, temp_file2 = tempfile.mkstemp(suffix=".blah", prefix=tmp_prefix)
    _, temp_file3 = tempfile.mkstemp(dir=temp_dir1, suffix=".metta", prefix=tmp_prefix)
    _, temp_file4 = tempfile.mkstemp(dir=temp_dir1, suffix=".metta", prefix=tmp_prefix)
    _, temp_file5 = tempfile.mkstemp(dir=temp_dir1, suffix=".blah", prefix=tmp_prefix)
    temp_dir2 = tempfile.mkdtemp(dir=temp_dir1, prefix=tmp_prefix)
    _, temp_file6 = tempfile.mkstemp(dir=temp_dir2, suffix=".metta", prefix=tmp_prefix)
    temp_dir3 = tempfile.mkdtemp(dir=temp_dir1, prefix=tmp_prefix)

    das = DistributedAtomSpace()
    #das.load_knowledge_base(temp_dir_1)

    file_list = das._get_file_list(temp_file1)
    assert len(file_list) == 1
    assert temp_file1 in file_list

    with pytest.raises(ValueError):
        file_list = das._get_file_list(temp_file2)

    file_list = das._get_file_list(temp_dir1)
    assert len(file_list) == 2
    assert temp_file3 in file_list
    assert temp_file4 in file_list

    with pytest.raises(ValueError):
        file_list = das._get_file_list(temp_dir3)

    shutil.rmtree(temp_dir1)
    os.remove(temp_file1)
    os.remove(temp_file2)

#def test_file_parse():
#    das = DistributedAtomSpace(knowledge_base_file_name="./data/samples/simple.metta")
