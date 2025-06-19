import os
from typing import Dict

class PortfolioReporter:
    """포트폴리오 리포트 생성 클래스"""
    
    def __init__(self, portfolio_manager):
        self.pm = portfolio_manager
    
    def generate_report(self, save_to_file: bool = True) -> str:
        """포트폴리오 리포트 생성"""
        try:
            summary = self.pm.utils.get_portfolio_summary()
            strategy_summary = summary.get('strategies', {})
            
            report = self._generate_report_content(summary, strategy_summary)
            
            if save_to_file:
                report_file = os.path.join(self.pm.portfolio_dir, f'{self.pm.portfolio_name}_report.md')
                with open(report_file, 'w', encoding='utf-8') as f:
                    f.write(report)
                print(f"📄 리포트 저장: {report_file}")
            
            return report
            
        except Exception as e:
            print(f"❌ 리포트 생성 실패: {e}")
            return ""
    
    def _generate_report_content(self, summary: Dict, strategy_summary: Dict) -> str:
        """리포트 내용 생성"""
        report = f"""
# 포트폴리오 리포트: {self.pm.portfolio_name}
생성일시: {summary.get('last_updated', 'N/A')}

## 📊 포트폴리오 개요
- 초기 자본: ${summary.get('initial_capital', 0):,.2f}
- 현재 가치: ${summary.get('current_value', 0):,.2f}
- 총 수익: ${summary.get('total_return', 0):,.2f} ({summary.get('total_return_pct', 0):.2f}%)

## 📈 포지션 현황
- 총 포지션: {summary.get('positions', {}).get('total_positions', 0)}개
- 롱 포지션: {summary.get('positions', {}).get('long_positions', 0)}개
- 숏 포지션: {summary.get('positions', {}).get('short_positions', 0)}개
- 미실현 손익: ${summary.get('positions', {}).get('total_unrealized_pnl', 0):,.2f}

## 🎯 전략별 현황
"""
        
        for strategy_name, strategy_data in strategy_summary.items():
            report += f"""
### {strategy_data['name']} ({strategy_name})
- 타입: {strategy_data['type']}
- 포지션 수: {strategy_data['position_count']}개
- 총 가치: ${strategy_data['total_value']:,.2f}
- 포트폴리오 비중: {strategy_data['weight']:.1%}
- 평균 수익률: {strategy_data['avg_pnl_pct']:.2f}%
- 미실현 손익: ${strategy_data['total_pnl']:,.2f}
"""
        
        report += f"""

## ⚠️ 리스크 관리
- 포트폴리오 VaR: {summary.get('risk', {}).get('var_percentage', 0):.2f}%
- 활성 스탑 오더: {summary.get('risk', {}).get('active_stop_orders', 0)}개
- 리스크 경고: {summary.get('risk', {}).get('risk_warnings', 0)}개

## 📊 성과 지표
- 총 거래: {summary.get('performance', {}).get('total_trades', 0)}회
- 승률: {summary.get('performance', {}).get('win_rate', 0):.1f}%
- 평균 보유일: {summary.get('performance', {}).get('avg_holding_days', 0):.1f}일
- 최고 수익: ${summary.get('performance', {}).get('best_trade', 0):,.2f}
- 최대 손실: ${summary.get('performance', {}).get('worst_trade', 0):,.2f}
"""
        
        return report