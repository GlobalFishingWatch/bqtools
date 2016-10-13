import os
import pytest

def pytest_addoption(parser):
    parser.addoption("--proj-id", action="store", help="GCS project")
    parser.addoption("--gcs-temp-dir", action="store",
        help="temporary directory for extracting tables to during test")
        
@pytest.fixture
def proj_id(request):
    return request.config.getoption("--proj-id")
        
@pytest.fixture
def gcs_temp_dir(request):
    return request.config.getoption("--gcs-temp-dir")
    
@pytest.fixture
def std_gcs_path(request):
    gcs_temp_dir = request.config.getoption("--gcs-temp-dir")  
    return os.path.join(gcs_temp_dir, 'test_bqtools.csv')
