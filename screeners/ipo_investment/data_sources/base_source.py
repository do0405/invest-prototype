"""IPO 데이터 소스 기본 클래스"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any
import time
import logging

logger = logging.getLogger(__name__)

class BaseIPODataSource(ABC):
    """IPO 데이터 소스 기본 클래스"""
    
    def __init__(self, request_delay: float = 1.0, max_retries: int = 3):
        self.request_delay = request_delay
        self.max_retries = max_retries
        
    def _safe_request_delay(self):
        """API 호출 간 안전한 지연"""
        time.sleep(self.request_delay)
        
    @abstractmethod
    def get_recent_ipos(self, months_back: int = 3) -> List[Dict[str, Any]]:
        """최근 IPO 데이터 수집"""
        pass
        
    @abstractmethod
    def get_upcoming_ipos(self, months_ahead: int = 3) -> List[Dict[str, Any]]:
        """예정된 IPO 데이터 수집"""
        pass
        
    @abstractmethod
    def is_available(self) -> bool:
        """데이터 소스 사용 가능 여부 확인"""
        pass