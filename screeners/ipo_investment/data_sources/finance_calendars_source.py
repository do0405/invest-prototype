"""Finance Calendars 데이터 소스"""

from typing import List, Dict, Any
import logging
from datetime import datetime
from .base_source import BaseIPODataSource

try:
    import finance_calendars as fc
except ImportError:
    fc = None

logger = logging.getLogger(__name__)

class FinanceCalendarsSource(BaseIPODataSource):
    """Finance Calendars를 사용한 IPO 데이터 소스"""
    
    def is_available(self) -> bool:
        """finance_calendars 라이브러리 사용 가능 여부 확인"""
        return fc is not None
        
    def get_recent_ipos(self, months_back: int = 3) -> List[Dict[str, Any]]:
        """finance_calendars를 사용하여 최근 IPO 데이터 수집"""
        if not self.is_available():
            logger.warning("finance_calendars 라이브러리가 없어 건너뜁니다.")
            return []
        
        recent_ipos = []
        
        try:
            # Get recent IPOs from finance_calendars
            recent_ipos_df = fc.get_priced_ipos_this_month()
            
            if not recent_ipos_df.empty:
                for _, ipo in recent_ipos_df.iterrows():
                    ipo_data = {
                        'symbol': '',  # ticker not available in this API
                        'company_name': ipo.get('companyName', ''),
                        'ipo_date': ipo.get('pricedDate', ''),
                        'price_range': f"${ipo.get('proposedSharePrice', '')}",
                        'shares_offered': ipo.get('sharesOffered', ''),
                        'exchange': ipo.get('proposedExchange', ''),
                        'sector': '',  # sector not available in this API
                        'type': 'IPO',
                        'source': 'finance_calendars'
                    }
                    recent_ipos.append(ipo_data)
                
        except Exception as e:
            logger.error(f"Error getting recent IPOs from finance_calendars: {e}")
            
        try:
            # Get filed IPOs from finance_calendars (as additional recent data)
            filed_ipos_df = fc.get_filed_ipos_this_month()
            
            if not filed_ipos_df.empty:
                for _, ipo in filed_ipos_df.iterrows():
                    ipo_data = {
                        'symbol': '',  # ticker not available in this API
                        'company_name': ipo.get('companyName', ''),
                        'ipo_date': ipo.get('filedDate', ''),
                        'price_range': f"${ipo.get('proposedSharePrice', '')}",
                        'shares_offered': ipo.get('sharesOffered', ''),
                        'exchange': ipo.get('proposedExchange', ''),
                        'sector': '',  # sector not available in this API
                        'type': 'IPO',
                        'source': 'finance_calendars_filed'
                    }
                    recent_ipos.append(ipo_data)
                
        except Exception as e:
            logger.error(f"Error getting filed IPOs from finance_calendars: {e}")
        
        logger.info(f"finance_calendars에서 총 {len(recent_ipos)}개 최근 IPO 수집")
        return recent_ipos
        
    def get_upcoming_ipos(self, months_ahead: int = 3) -> List[Dict[str, Any]]:
        """finance_calendars를 사용하여 예정된 IPO 데이터 수집"""
        if not self.is_available():
            logger.warning("finance_calendars 라이브러리가 없어 건너뜁니다.")
            return []
        
        upcoming_ipos = []
        
        try:
            # Get upcoming IPOs from finance_calendars
            upcoming_ipos_df = fc.get_upcoming_ipos_this_month()
            
            if not upcoming_ipos_df.empty:
                for _, ipo in upcoming_ipos_df.iterrows():
                    ipo_data = {
                        'symbol': '',  # ticker not available in this API
                        'company_name': ipo.get('companyName', ''),
                        'ipo_date': ipo.get('expectedPriceDate', ''),
                        'price_range': f"${ipo.get('proposedSharePrice', '')}",
                        'shares_offered': ipo.get('sharesOffered', ''),
                        'exchange': ipo.get('proposedExchange', ''),
                        'sector': '',  # sector not available in this API
                        'type': 'IPO',
                        'source': 'finance_calendars_upcoming'
                    }
                    upcoming_ipos.append(ipo_data)
                
        except Exception as e:
            logger.error(f"Error getting upcoming IPOs from finance_calendars: {e}")
        
        logger.info(f"finance_calendars에서 총 {len(upcoming_ipos)}개 예정 IPO 수집")
        return upcoming_ipos