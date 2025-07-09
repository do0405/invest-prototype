"""Investpy 데이터 소스"""

from typing import List, Dict, Any
import logging
import warnings
from .base_source import BaseIPODataSource

# pkg_resources 경고 억제
warnings.filterwarnings('ignore', message='pkg_resources is deprecated')

try:
    import investpy
except ImportError:
    investpy = None

logger = logging.getLogger(__name__)

class InvestpySource(BaseIPODataSource):
    """Investpy를 사용한 IPO 데이터 소스"""
    
    def is_available(self) -> bool:
        """investpy 라이브러리 사용 가능 여부 확인"""
        return investpy is not None
        
    def get_recent_ipos(self, months_back: int = 3) -> List[Dict[str, Any]]:
        """investpy를 사용하여 IPO 데이터 수집 (보조적)"""
        if not self.is_available():
            logger.warning("investpy 라이브러리가 없어 건너뜁니다.")
            return []
        
        ipos_data = []
        
        try:
            # investpy는 주로 주식 데이터에 특화되어 있어 IPO 전용 기능이 제한적
            # 대신 최근 상장된 주식들을 검색하여 IPO 정보를 추론
            self._safe_request_delay()
            
            # 미국 주식 중 최근 상장된 것들 검색
            search_results = investpy.search_quotes(
                text='IPO', 
                products=['stocks'], 
                countries=['united states'], 
                n_results=10
            )
            
            if isinstance(search_results, list):
                for result in search_results:
                    try:
                        info = result.retrieve_information()
                        ipo_data = {
                            'symbol': result.symbol,
                            'company_name': result.name,
                            'ipo_date': 'N/A',  # investpy에서 직접 IPO 날짜 제공 안함
                            'price_range': 'N/A',
                            'shares_offered': 0,
                            'estimated_market_cap': 0,
                            'exchange': getattr(result, 'exchange', 'N/A'),
                            'sector': info.get('Sector', 'N/A') if info else 'N/A',
                            'type': 'IPO',
                            'source': 'investpy'
                        }
                        ipos_data.append(ipo_data)
                    except Exception as e:
                        logger.warning(f"investpy 개별 데이터 처리 실패: {e}")
                        continue
            
        except Exception as e:
            logger.error(f"investpy IPO 데이터 수집 실패: {e}")
        
        logger.info(f"investpy에서 총 {len(ipos_data)}개 IPO 관련 데이터 수집")
        return ipos_data
        
    def get_upcoming_ipos(self, months_ahead: int = 3) -> List[Dict[str, Any]]:
        """investpy는 예정된 IPO 데이터를 제공하지 않음"""
        logger.info("investpy는 예정된 IPO 데이터를 제공하지 않습니다.")
        return []