# -*- coding: utf-8 -*-
"""
트레일링 스탑 관리 모듈
포지션의 트레일링 스탑 가격을 계산하고 업데이트하는 기능 제공
"""

import os
import pandas as pd
from datetime import datetime
from typing import Dict, Optional, Tuple

class TrailingStopManager:
    """트레일링 스탑 관리 클래스"""
    
    def __init__(self, portfolio_dir: str, portfolio_name: str):
        self.portfolio_dir = portfolio_dir
        self.portfolio_name = portfolio_name
        self.trailing_stops_file = os.path.join(portfolio_dir, f"{portfolio_name}_trailing_stops.csv")
        self.trailing_stops = self._load_trailing_stops()
    
    def _load_trailing_stops(self) -> pd.DataFrame:
        """트레일링 스탑 데이터 로드"""
        if os.path.exists(self.trailing_stops_file):
            try:
                return pd.read_csv(self.trailing_stops_file)
            except Exception as e:
                print(f"⚠️ 트레일링 스탑 데이터 로드 실패: {e}")
        
        # 파일이 없으면 빈 DataFrame 생성
        return pd.DataFrame(columns=[
            'symbol', 'position_type', 'strategy', 'entry_price', 'entry_date',
            'highest_price', 'lowest_price', 'trailing_stop_price', 'trailing_pct',
            'last_updated'
        ])
    
    def save_trailing_stops(self):
        """트레일링 스탑 데이터 저장"""
        try:
            self.trailing_stops.to_csv(self.trailing_stops_file, index=False)
        except Exception as e:
            print(f"⚠️ 트레일링 스탑 데이터 저장 실패: {e}")
    
    def initialize_trailing_stop(self, symbol: str, position_type: str, strategy: str, 
                               entry_price: float, entry_date: str, trailing_pct: float) -> float:
        """트레일링 스탑 초기화
        
        Args:
            symbol: 종목 심볼
            position_type: 포지션 타입 (BUY 또는 SELL)
            strategy: 전략 이름
            entry_price: 진입 가격
            entry_date: 진입 날짜
            trailing_pct: 트레일링 스탑 퍼센트 (소수점 형태, 예: 0.1 = 10%)
            
        Returns:
            초기 트레일링 스탑 가격
        """
        try:
            # 초기 트레일링 스탑 가격 계산
            if position_type == 'BUY':
                trailing_stop_price = entry_price * (1 - trailing_pct)
                highest_price = entry_price
                lowest_price = entry_price
            else:  # SELL
                trailing_stop_price = entry_price * (1 + trailing_pct)
                highest_price = entry_price
                lowest_price = entry_price
            
            # 기존 데이터 확인
            mask = (self.trailing_stops['symbol'] == symbol) & \
                   (self.trailing_stops['position_type'] == position_type) & \
                   (self.trailing_stops['strategy'] == strategy)
            
            if mask.any():
                # 기존 데이터 업데이트
                idx = mask.idxmax()
                self.trailing_stops.loc[idx, 'entry_price'] = entry_price
                self.trailing_stops.loc[idx, 'entry_date'] = entry_date
                self.trailing_stops.loc[idx, 'highest_price'] = highest_price
                self.trailing_stops.loc[idx, 'lowest_price'] = lowest_price
                self.trailing_stops.loc[idx, 'trailing_stop_price'] = trailing_stop_price
                self.trailing_stops.loc[idx, 'trailing_pct'] = trailing_pct
                self.trailing_stops.loc[idx, 'last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            else:
                # 새 데이터 추가
                new_data = {
                    'symbol': symbol,
                    'position_type': position_type,
                    'strategy': strategy,
                    'entry_price': entry_price,
                    'entry_date': entry_date,
                    'highest_price': highest_price,
                    'lowest_price': lowest_price,
                    'trailing_stop_price': trailing_stop_price,
                    'trailing_pct': trailing_pct,
                    'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                self.trailing_stops = pd.concat([self.trailing_stops, pd.DataFrame([new_data])], ignore_index=True)
            
            self.save_trailing_stops()
            return trailing_stop_price
            
        except Exception as e:
            print(f"⚠️ 트레일링 스탑 초기화 실패 ({symbol}): {e}")
            # 기본 트레일링 스탑 가격 반환
            if position_type == 'BUY':
                return entry_price * 0.9  # 기본 10% 하락
            else:
                return entry_price * 1.1  # 기본 10% 상승
    
    def update_trailing_stop(self, symbol: str, position_type: str, strategy: str, 
                           current_price: float) -> Optional[float]:
        """트레일링 스탑 업데이트
        
        Args:
            symbol: 종목 심볼
            position_type: 포지션 타입 (BUY 또는 SELL)
            strategy: 전략 이름
            current_price: 현재 가격
            
        Returns:
            업데이트된 트레일링 스탑 가격 또는 None (데이터 없음)
        """
        try:
            # 기존 데이터 확인
            mask = (self.trailing_stops['symbol'] == symbol) & \
                   (self.trailing_stops['position_type'] == position_type) & \
                   (self.trailing_stops['strategy'] == strategy)
            
            if not mask.any():
                return None
            
            idx = mask.idxmax()
            row = self.trailing_stops.loc[idx]
            trailing_pct = row['trailing_pct']
            
            if position_type == 'BUY':
                # 롱 포지션: 최고가 업데이트 및 트레일링 스탑 상향 조정
                highest_price = row['highest_price']
                
                if current_price > highest_price:
                    # 최고가 갱신 시 트레일링 스탑 상향 조정
                    new_highest_price = current_price
                    new_trailing_stop = current_price * (1 - trailing_pct)
                    
                    # 기존 트레일링 스탑보다 높은 경우에만 업데이트
                    if new_trailing_stop > row['trailing_stop_price']:
                        self.trailing_stops.loc[idx, 'highest_price'] = new_highest_price
                        self.trailing_stops.loc[idx, 'trailing_stop_price'] = new_trailing_stop
                        self.trailing_stops.loc[idx, 'last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        self.save_trailing_stops()
                        return new_trailing_stop
            
            else:  # SELL
                # 숏 포지션: 최저가 업데이트 및 트레일링 스탑 하향 조정
                lowest_price = row['lowest_price']
                
                if current_price < lowest_price:
                    # 최저가 갱신 시 트레일링 스탑 하향 조정
                    new_lowest_price = current_price
                    new_trailing_stop = current_price * (1 + trailing_pct)
                    
                    # 기존 트레일링 스탑보다 낮은 경우에만 업데이트
                    if new_trailing_stop < row['trailing_stop_price']:
                        self.trailing_stops.loc[idx, 'lowest_price'] = new_lowest_price
                        self.trailing_stops.loc[idx, 'trailing_stop_price'] = new_trailing_stop
                        self.trailing_stops.loc[idx, 'last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        self.save_trailing_stops()
                        return new_trailing_stop
            
            # 변경 없음
            return row['trailing_stop_price']
            
        except Exception as e:
            print(f"⚠️ 트레일링 스탑 업데이트 실패 ({symbol}): {e}")
            return None
    
    def get_trailing_stop(self, symbol: str, position_type: str, strategy: str) -> Optional[float]:
        """트레일링 스탑 가격 조회
        
        Args:
            symbol: 종목 심볼
            position_type: 포지션 타입 (BUY 또는 SELL)
            strategy: 전략 이름
            
        Returns:
            트레일링 스탑 가격 또는 None (데이터 없음)
        """
        try:
            mask = (self.trailing_stops['symbol'] == symbol) & \
                   (self.trailing_stops['position_type'] == position_type) & \
                   (self.trailing_stops['strategy'] == strategy)
            
            if mask.any():
                return self.trailing_stops.loc[mask.idxmax(), 'trailing_stop_price']
            return None
            
        except Exception as e:
            print(f"⚠️ 트레일링 스탑 조회 실패 ({symbol}): {e}")
            return None
    
    def check_trailing_stop_hit(self, symbol: str, position_type: str, strategy: str, 
                              current_price: float) -> Tuple[bool, float]:
        """트레일링 스탑 도달 여부 확인
        
        Args:
            symbol: 종목 심볼
            position_type: 포지션 타입 (BUY 또는 SELL)
            strategy: 전략 이름
            current_price: 현재 가격
            
        Returns:
            (도달 여부, 트레일링 스탑 가격)
        """
        try:
            trailing_stop = self.get_trailing_stop(symbol, position_type, strategy)
            
            if trailing_stop is None:
                return False, 0
            
            if position_type == 'BUY':
                # 롱 포지션: 현재가가 트레일링 스탑 이하면 청산
                return current_price <= trailing_stop, trailing_stop
            else:  # SELL
                # 숏 포지션: 현재가가 트레일링 스탑 이상이면 청산
                return current_price >= trailing_stop, trailing_stop
                
        except Exception as e:
            print(f"⚠️ 트레일링 스탑 도달 확인 실패 ({symbol}): {e}")
            return False, 0
    
    def remove_trailing_stop(self, symbol: str, position_type: str, strategy: str):
        """트레일링 스탑 제거 (포지션 청산 시)"""
        try:
            mask = (self.trailing_stops['symbol'] == symbol) & \
                   (self.trailing_stops['position_type'] == position_type) & \
                   (self.trailing_stops['strategy'] == strategy)
            
            if mask.any():
                self.trailing_stops = self.trailing_stops[~mask].reset_index(drop=True)
                self.save_trailing_stops()
                
        except Exception as e:
            print(f"⚠️ 트레일링 스탑 제거 실패 ({symbol}): {e}")