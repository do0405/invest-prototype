'use client';

import Link from 'next/link';

interface ScreenerInfo {
  id: string;
  name: string;
  description: string;
  criteriaCount: number;
  color: string;
  icon: string;
}

const screenerData: ScreenerInfo[] = [
  {
    id: 'advanced_financial_results',
    name: 'Advanced Financial',
    description: '매출·이익 성장률, ROE, 부채비율 등 재무 건전성 중심',
    criteriaCount: 5,
    color: 'from-blue-500 to-blue-600',
    icon: '📊'
  },
  {
    id: 'integrated_results',
    name: 'Integrated Analysis',
    description: '기술적·펀더멘털·모멘텀 점수를 종합한 통합 분석',
    criteriaCount: 3,
    color: 'from-purple-500 to-purple-600',
    icon: '🎯'
  },
  {
    id: 'pattern_detection_results',
    name: 'Pattern Detection',
    description: 'VCP, 컵앤핸들 등 차트 패턴 기반 수축 신호 탐지',
    criteriaCount: 5,
    color: 'from-green-500 to-green-600',
    icon: '📈'
  },
  {
    id: 'us_with_rs',
    name: 'Relative Strength',
    description: 'RS Rating, 가격 성과, 거래량 급증 등 상대강도 분석',
    criteriaCount: 4,
    color: 'from-orange-500 to-orange-600',
    icon: '🚀'
  },
  {
    id: 'new_tickers',
    name: 'New Tickers',
    description: '신규 상장 종목 중 성장 잠재력이 높은 종목',
    criteriaCount: 3,
    color: 'from-teal-500 to-teal-600',
    icon: '✨'
  },
  {
    id: 'previous_us_with_rs',
    name: 'Previous RS Analysis',
    description: '이전 기간 상대강도 분석 결과',
    criteriaCount: 4,
    color: 'from-gray-500 to-gray-600',
    icon: '📋'
  }
];

export default function ScreeningSummary() {
  // const [selectedScreener, setSelectedScreener] = useState<string | null>(null);

  return (
    <div className="bg-white rounded-xl shadow-lg border border-gray-200 overflow-hidden">
      {/* Header */}
      <div className="bg-gradient-to-r from-indigo-600 to-purple-600 px-6 py-4">
        <div className="flex items-center space-x-3">
          <div className="w-10 h-10 bg-white bg-opacity-20 rounded-lg flex items-center justify-center">
            <span className="text-xl">🔍</span>
          </div>
          <div>
            <h2 className="text-xl font-bold text-white">Mark Minervini 스크리닝 시스템</h2>
            <p className="text-indigo-100 text-sm">성장주 발굴을 위한 다차원 분석 도구</p>
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="p-6">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {screenerData.map((screener) => (
            <Link
              key={screener.id}
              href={`/markminervini/${screener.id}`}
              className="group block"
            >
              <div className="bg-gradient-to-br from-gray-50 to-white border border-gray-200 rounded-lg p-4 hover:shadow-lg transition-all duration-300 hover:scale-105 cursor-pointer">
                <div className="flex items-start justify-between mb-3">
                  <div className={`w-12 h-12 bg-gradient-to-br ${screener.color} rounded-lg flex items-center justify-center text-white text-xl shadow-md`}>
                    {screener.icon}
                  </div>
                  <div className="text-right">
                    <span className="px-2 py-1 bg-blue-100 text-blue-700 text-xs font-medium rounded-full">
                      {screener.criteriaCount}개 기준
                    </span>
                  </div>
                </div>
                
                <h3 className="font-semibold text-gray-800 mb-2 group-hover:text-indigo-600 transition-colors">
                  {screener.name}
                </h3>
                
                <p className="text-gray-600 text-sm leading-relaxed">
                  {screener.description}
                </p>
                
                <div className="mt-4 flex items-center justify-between">
                  <span className="text-xs text-gray-500">자세히 보기</span>
                  <div className="w-6 h-6 bg-gray-100 rounded-full flex items-center justify-center group-hover:bg-indigo-100 transition-colors">
                    <span className="text-gray-600 group-hover:text-indigo-600 text-sm">→</span>
                  </div>
                </div>
              </div>
            </Link>
          ))}
        </div>

        {/* Quick Stats */}
        <div className="mt-8 bg-gradient-to-r from-blue-50 to-indigo-50 rounded-lg p-6">
          <h3 className="text-lg font-semibold text-gray-800 mb-4 flex items-center space-x-2">
            <span>📋</span>
            <span>스크리닝 개요</span>
          </h3>
          
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div className="text-center">
              <div className="text-2xl font-bold text-blue-600 mb-1">{screenerData.length}</div>
              <div className="text-sm text-gray-600">총 스크리너 수</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-green-600 mb-1">
                {screenerData.reduce((sum, s) => sum + s.criteriaCount, 0)}
              </div>
              <div className="text-sm text-gray-600">총 분석 기준</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-purple-600 mb-1">100%</div>
              <div className="text-sm text-gray-600">자동화 수준</div>
            </div>
          </div>
        </div>

        {/* Key Features */}
        <div className="mt-6 grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="bg-green-50 border border-green-200 rounded-lg p-4">
            <div className="flex items-center space-x-2 mb-2">
              <span className="text-green-600">✅</span>
              <span className="font-medium text-green-800">실시간 분석</span>
            </div>
            <p className="text-green-700 text-sm">시장 데이터 기반 실시간 종목 선별</p>
          </div>
          
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
            <div className="flex items-center space-x-2 mb-2">
              <span className="text-blue-600">🎯</span>
              <span className="font-medium text-blue-800">다차원 필터링</span>
            </div>
            <p className="text-blue-700 text-sm">기술적·펀더멘털·모멘텀 종합 분석</p>
          </div>
        </div>
      </div>
    </div>
  );
}