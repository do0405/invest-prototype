'use client';

import { useState } from 'react';
import { ChevronDownIcon, ChevronUpIcon } from '@heroicons/react/24/outline';

interface CriteriaItem {
  id: string;
  name: string;
  description: string;
  threshold: string;
  weight: number;
}

interface ScreeningCriteriaProps {
  screenerId: string;
}

const criteriaData: { [key: string]: CriteriaItem[] } = {
  'advanced_financial_results': [
    {
      id: 'revenue_growth',
      name: '매출 성장률',
      description: '최근 분기 매출이 전년 동기 대비 증가',
      threshold: '> 15%',
      weight: 25
    },
    {
      id: 'earnings_growth',
      name: '순이익 성장률',
      description: '최근 분기 순이익이 전년 동기 대비 증가',
      threshold: '> 20%',
      weight: 30
    },
    {
      id: 'roe',
      name: '자기자본이익률 (ROE)',
      description: '자기자본 대비 순이익 비율',
      threshold: '> 17%',
      weight: 20
    },
    {
      id: 'debt_ratio',
      name: '부채비율',
      description: '총부채 대비 총자산 비율',
      threshold: '< 40%',
      weight: 15
    },
    {
      id: 'current_ratio',
      name: '유동비율',
      description: '유동자산 대비 유동부채 비율',
      threshold: '> 1.5',
      weight: 10
    }
  ],
  'integrated_results': [
    {
      id: 'technical_score',
      name: '기술적 점수',
      description: '차트 패턴 및 기술적 지표 종합 점수',
      threshold: '> 70점',
      weight: 40
    },
    {
      id: 'fundamental_score',
      name: '펀더멘털 점수',
      description: '재무 건전성 및 성장성 종합 점수',
      threshold: '> 75점',
      weight: 35
    },
    {
      id: 'momentum_score',
      name: '모멘텀 점수',
      description: '가격 및 거래량 모멘텀 점수',
      threshold: '> 65점',
      weight: 25
    }
  ],
  'pattern_detection_results': [
    {
      id: 'vdu',
      name: '거래량 Dry-Up (VDU)',
      description: '최근 거래량이 평균 대비 감소',
      threshold: '< 40% of EMA50',
      weight: 20
    },
    {
      id: 'price_contraction',
      name: '가격 범위 축소',
      description: '최근 가격 변동폭이 이전 대비 축소',
      threshold: '< 80% of prev range',
      weight: 20
    },
    {
      id: 'volatility_contraction',
      name: '변동성 수축',
      description: 'ATR 지표 기반 변동성 감소',
      threshold: '< 80% of prev ATR',
      weight: 20
    },
    {
      id: 'volume_downtrend',
      name: '거래량 하락 추세',
      description: '지속적인 거래량 감소 패턴',
      threshold: 'Slope < -0.001',
      weight: 30
    },
    {
      id: 'higher_lows',
      name: 'Higher Lows 패턴',
      description: '연속적으로 상승하는 저점 형성',
      threshold: '3연속 상승',
      weight: 10
    }
  ],
  'us_with_rs': [
    {
      id: 'rs_rating',
      name: 'RS Rating',
      description: '상대강도 지수 (시장 대비 성과)',
      threshold: '> 80',
      weight: 40
    },
    {
      id: 'price_performance',
      name: '가격 성과',
      description: '최근 3개월 가격 상승률',
      threshold: '> 30%',
      weight: 30
    },
    {
      id: 'volume_surge',
      name: '거래량 급증',
      description: '평균 거래량 대비 증가율',
      threshold: '> 150%',
      weight: 20
    },
    {
      id: 'breakout_pattern',
      name: '돌파 패턴',
      description: '저항선 돌파 여부',
      threshold: '신고점 근접',
      weight: 10
    }
  ]
};

const getDefaultCriteria = (): CriteriaItem[] => [
  {
    id: 'general_screening',
    name: '일반 스크리닝',
    description: '기본적인 주식 선별 기준',
    threshold: '다양한 조건',
    weight: 100
  }
];

export default function ScreeningCriteria({ screenerId }: ScreeningCriteriaProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  
  const criteria = criteriaData[screenerId] || getDefaultCriteria();
  const totalWeight = criteria.reduce((sum, item) => sum + item.weight, 0);

  return (
    <div className="bg-gradient-to-br from-blue-50 to-indigo-50 rounded-xl border border-blue-200 overflow-hidden">
      {/* Header */}
      <div 
        className="px-6 py-4 bg-gradient-to-r from-blue-600 to-indigo-600 text-white cursor-pointer hover:from-blue-700 hover:to-indigo-700 transition-all duration-300"
        onClick={() => setIsExpanded(!isExpanded)}
      >
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <div className="w-8 h-8 bg-white bg-opacity-20 rounded-lg flex items-center justify-center">
              <span className="text-lg">🎯</span>
            </div>
            <div>
              <h3 className="text-lg font-semibold">스크리닝 기준</h3>
              <p className="text-blue-100 text-sm">{criteria.length}개 조건 • 총 {totalWeight}점</p>
            </div>
          </div>
          <div className="flex items-center space-x-2">
            <span className="text-sm text-blue-100">
              {isExpanded ? '접기' : '자세히 보기'}
            </span>
            {isExpanded ? (
              <ChevronUpIcon className="w-5 h-5 text-blue-200" />
            ) : (
              <ChevronDownIcon className="w-5 h-5 text-blue-200" />
            )}
          </div>
        </div>
      </div>

      {/* Content */}
      <div className={`transition-all duration-500 ease-in-out overflow-hidden ${
        isExpanded ? 'max-h-96 opacity-100' : 'max-h-0 opacity-0'
      }`}>
        <div className="p-6 space-y-4">
          {criteria.map((criterion, index) => (
            <div 
              key={criterion.id}
              className="bg-white rounded-lg p-4 shadow-sm border border-gray-100 hover:shadow-md transition-shadow duration-200"
            >
              <div className="flex items-start justify-between">
                <div className="flex-1">
                  <div className="flex items-center space-x-3 mb-2">
                    <div className="w-6 h-6 bg-blue-100 text-blue-600 rounded-full flex items-center justify-center text-sm font-semibold">
                      {index + 1}
                    </div>
                    <h4 className="font-semibold text-gray-800">{criterion.name}</h4>
                    <div className="flex items-center space-x-1">
                      <span className="text-xs text-gray-500">가중치</span>
                      <span className="px-2 py-1 bg-blue-100 text-blue-700 text-xs font-medium rounded-full">
                        {criterion.weight}점
                      </span>
                    </div>
                  </div>
                  <p className="text-gray-600 text-sm mb-2">{criterion.description}</p>
                  <div className="flex items-center space-x-2">
                    <span className="text-xs text-gray-500">기준값:</span>
                    <code className="px-2 py-1 bg-gray-100 text-gray-700 text-xs rounded font-mono">
                      {criterion.threshold}
                    </code>
                  </div>
                </div>
                <div className="ml-4">
                  <div className="w-12 h-12 bg-gradient-to-br from-green-100 to-emerald-100 rounded-lg flex items-center justify-center">
                    <span className="text-green-600 text-lg">✓</span>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Summary when collapsed */}
      {!isExpanded && (
        <div className="px-6 py-3 bg-white border-t border-blue-100">
          <div className="flex items-center justify-between text-sm">
            <span className="text-gray-600">주요 기준: {criteria.slice(0, 2).map(c => c.name).join(', ')}</span>
            <span className="text-blue-600 font-medium">클릭하여 전체 기준 보기</span>
          </div>
        </div>
      )}
    </div>
  );
}