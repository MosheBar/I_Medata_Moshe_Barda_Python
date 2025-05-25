"""
Base API client with retry logic and error handling.
"""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import allure
from typing import Optional, Dict, Any, Union
from config.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class APIError(Exception):
    """Custom API Exception"""
    def __init__(self, message: str, status_code: Optional[int] = None, response: Optional[requests.Response] = None):
        self.message = message
        self.status_code = status_code
        self.response = response
        super().__init__(self.message)

class BaseAPIClient:
    def __init__(self):
        self.config = config
        self.base_url = self.config.api_base_url
        self.session = self._create_session()
        self.logger = logger

    def _create_session(self) -> requests.Session:
        """Create a session with retry logic"""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.config.retry_count,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(self.config.request_headers)
        return session

    @allure.step("Making {method} request to {endpoint}")
    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
    ) -> requests.Response:
        """Make HTTP request with logging and error handling"""
        url = f"{self.base_url}/{self.config.api_version.strip('/')}/{endpoint.strip('/')}"
        timeout = timeout or self.config.timeout
        
        self.logger.info(f"Making {method} request to {url}")
        if params:
            self.logger.debug(f"Request params: {params}")
        if data:
            self.logger.debug(f"Request data: {data}")
        if json:
            self.logger.debug(f"Request json: {json}")

        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                data=data,
                json=json,
                headers=headers,
                timeout=timeout,
                verify=self.config.verify_ssl
            )
            
            # Log response details
            self.logger.info(f"Response status code: {response.status_code}")
            self.logger.debug(f"Response headers: {response.headers}")
            self.logger.debug(f"Response body: {response.text}")
            
            # Attach response to Allure report
            allure.attach(
                response.text,
                name=f"Response {response.status_code}",
                attachment_type=allure.attachment_type.TEXT
            )
            
            # Raise for status
            response.raise_for_status()
            return response

        except requests.exceptions.RequestException as e:
            error_msg = f"Request failed: {str(e)}"
            self.logger.error(error_msg)
            if hasattr(e, 'response') and e.response is not None:
                raise APIError(error_msg, e.response.status_code, e.response)
            raise APIError(error_msg)

    def get(self, endpoint: str, **kwargs) -> Union[Dict[str, Any], list]:
        """Send GET request"""
        response = self._make_request("GET", endpoint, **kwargs)
        return response.json()

    def post(self, endpoint: str, **kwargs) -> Union[Dict[str, Any], list]:
        """Send POST request"""
        response = self._make_request("POST", endpoint, **kwargs)
        return response.json()

    def put(self, endpoint: str, **kwargs) -> Union[Dict[str, Any], list]:
        """Send PUT request"""
        response = self._make_request("PUT", endpoint, **kwargs)
        return response.json()

    def delete(self, endpoint: str, **kwargs) -> Union[Dict[str, Any], list]:
        """Send DELETE request"""
        response = self._make_request("DELETE", endpoint, **kwargs)
        return response.json()

    def patch(self, endpoint: str, **kwargs) -> Union[Dict[str, Any], list]:
        """Send PATCH request"""
        response = self._make_request("PATCH", endpoint, **kwargs)
        return response.json() 