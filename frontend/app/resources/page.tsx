'use client';

import React from 'react';
import { motion } from 'framer-motion';
import { FaExternalLinkAlt, FaChartLine, FaNewspaper, FaUniversity, FaSearch } from 'react-icons/fa';

interface ResourceItem {
  id: string;
  name: string;
  description: string;
  url: string;
  category: string;
  icon: string;
  features: string[];
}

const resources: ResourceItem[] = [
  {
    id: 'tradingview-screener',
    name: 'TradingView 스크리너',
    description: '한국 TradingView의 강력한 주식 스크리닝 도구로 다양한 조건으로 종목을 필터링할 수 있습니다',
    url: 'https://kr.tradingview.com/screener/',
    category: 'screening',
    icon: '📊',
    features: ['실시간 스크리닝', '기술적 지표 필터', '재무 데이터 필터', '커스텀 조건 설정']
  }
];

const categoryIcons = {
  screening: <FaSearch className="w-5 h-5" />
};

const categoryNames = {
  screening: '🔍 스크리닝 도구'
};

const ResourcesPage: React.FC = () => {
  const categories = Array.from(new Set(resources.map(r => r.category)));

  return (
    <div className="min-h-screen bg-gray-50 py-8">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* 헤더 */}
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
          className="text-center mb-16"
        >
          <h1 className="text-5xl font-bold text-gray-900 mb-6">
            🔗 투자 스크리닝 도구
          </h1>
          <p className="text-xl text-gray-600 max-w-2xl mx-auto leading-relaxed">
            전문적인 주식 스크리닝과 시장 분석을 위한 필수 도구들
          </p>
        </motion.div>

        {/* 카테고리별 리소스 */}
        {categories.map((category, categoryIndex) => {
          const categoryResources = resources.filter(r => r.category === category);
          
          return (
            <motion.div
              key={category}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.6, delay: categoryIndex * 0.1 }}
              className="mb-12"
            >
              <div className="flex items-center mb-6">
                <div className="flex items-center justify-center w-10 h-10 bg-blue-100 rounded-lg mr-4">
                  {categoryIcons[category as keyof typeof categoryIcons]}
                </div>
                <h2 className="text-2xl font-bold text-gray-900">
                  {categoryNames[category as keyof typeof categoryNames]}
                </h2>
              </div>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-8 max-w-4xl mx-auto">
                {categoryResources.map((resource, index) => (
                  <motion.div
                    key={resource.id}
                    initial={{ opacity: 0, scale: 0.9 }}
                    animate={{ opacity: 1, scale: 1 }}
                    transition={{ duration: 0.4, delay: index * 0.2 }}
                    whileHover={{ scale: 1.05, y: -8 }}
                    whileTap={{ scale: 0.98 }}
                    className="group bg-gradient-to-br from-white to-gray-50 rounded-2xl shadow-lg hover:shadow-2xl transition-all duration-300 overflow-hidden border border-gray-100 hover:border-blue-200 p-8 text-center"
                  >
                    <div className="mb-6">
                      <span className="text-6xl mb-4 block group-hover:scale-110 transition-transform duration-300">
                        {resource.icon}
                      </span>
                      <h3 className="text-2xl font-bold text-gray-900 mb-3 group-hover:text-blue-600 transition-colors">
                        {resource.name}
                      </h3>
                      <p className="text-gray-600 leading-relaxed mb-6">
                        {resource.description}
                      </p>
                    </div>
                    
                    <div className="mb-6">
                      <div className="flex flex-wrap gap-2 justify-center">
                        {resource.features.map((feature, featureIndex) => (
                          <span
                            key={featureIndex}
                            className="px-3 py-1 bg-blue-100 text-blue-800 text-sm rounded-full group-hover:bg-blue-200 transition-colors"
                          >
                            {feature}
                          </span>
                        ))}
                      </div>
                    </div>
                    
                    <div className="flex gap-3">
                      <a
                        href="/embedded-screener"
                        className="flex-1 bg-green-600 hover:bg-green-700 text-white font-semibold py-3 px-4 rounded-xl transition-all duration-200 flex items-center justify-center hover:shadow-lg"
                      >
                        <span className="mr-2">🖥️</span>
                        <span className="text-sm">임베디드 뷰</span>
                      </a>
                      <a
                        href={resource.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="flex-1 bg-blue-600 hover:bg-blue-700 text-white font-semibold py-3 px-4 rounded-xl transition-all duration-200 flex items-center justify-center hover:shadow-lg"
                      >
                        <span className="mr-2">외부 링크</span>
                        <FaExternalLinkAlt className="w-3 h-3 hover:translate-x-1 transition-transform" />
                      </a>
                    </div>
                  </motion.div>
                ))}
              </div>
            </motion.div>
          );
        })}
        
        {/* 주의사항 */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6, delay: 0.8 }}
          className="bg-yellow-50 border border-yellow-200 rounded-xl p-6 mt-12"
        >
          <h3 className="text-lg font-semibold text-yellow-800 mb-3 flex items-center">
            ⚠️ 투자 주의사항
          </h3>
          <div className="text-yellow-700 space-y-2">
            <p>• 모든 투자 결정은 본인의 책임하에 이루어져야 합니다.</p>
            <p>• 제공된 정보는 참고용이며, 투자 권유가 아닙니다.</p>
            <p>• 투자 전 충분한 조사와 분석을 통해 신중하게 결정하시기 바랍니다.</p>
            <p>• 각 사이트의 이용약관과 개인정보처리방침을 확인하시기 바랍니다.</p>
          </div>
        </motion.div>
      </div>
    </div>
  );
};

export default ResourcesPage;