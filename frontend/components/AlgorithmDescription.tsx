'use client';
import { useState } from 'react';
import { ChevronDownIcon, ChevronUpIcon, InformationCircleIcon } from '@heroicons/react/24/outline';

interface AlgorithmStep {
  title: string;
  description: string;
  details?: string;
}

interface AlgorithmDescriptionProps {
  algorithm: string;
  className?: string;
}

export default function AlgorithmDescription({
  algorithm,
  className = ''
}: AlgorithmDescriptionProps) {
  // Import the algorithm descriptions
  const { getAlgorithmDescription } = require('@/lib/algorithmDescriptions');
  const algorithmData = getAlgorithmDescription(algorithm);
  
  if (!algorithmData) {
    return null;
  }
  
  const { title, summary, purpose, steps, dataFlow, coreLogic } = algorithmData;
  const [isExpanded, setIsExpanded] = useState(false);
  const [activeTab, setActiveTab] = useState<'overview' | 'steps' | 'implementation'>('overview');

  return (
    <div className={`bg-gradient-to-r from-blue-50 to-indigo-50 border border-blue-200 rounded-lg mb-6 ${className}`}>
      {/* Header */}
      <div 
        className="flex items-center justify-between p-4 cursor-pointer hover:bg-blue-100 transition-colors"
        onClick={() => setIsExpanded(!isExpanded)}
      >
        <div className="flex items-center space-x-3">
          <InformationCircleIcon className="h-6 w-6 text-blue-600" />
          <div>
            <h3 className="text-lg font-semibold text-gray-900">{title}</h3>
            <p className="text-sm text-gray-600">{summary}</p>
          </div>
        </div>
        {isExpanded ? (
          <ChevronUpIcon className="h-5 w-5 text-blue-600" />
        ) : (
          <ChevronDownIcon className="h-5 w-5 text-blue-600" />
        )}
      </div>

      {/* Expanded Content */}
      {isExpanded && (
        <div className="border-t border-blue-200">
          {/* Tab Navigation */}
          <div className="flex border-b border-blue-200">
            <button
              className={`px-4 py-2 text-sm font-medium transition-colors ${
                activeTab === 'overview'
                  ? 'text-blue-600 border-b-2 border-blue-600 bg-white'
                  : 'text-gray-600 hover:text-blue-600'
              }`}
              onClick={() => setActiveTab('overview')}
            >
              개요
            </button>
            <button
              className={`px-4 py-2 text-sm font-medium transition-colors ${
                activeTab === 'steps'
                  ? 'text-blue-600 border-b-2 border-blue-600 bg-white'
                  : 'text-gray-600 hover:text-blue-600'
              }`}
              onClick={() => setActiveTab('steps')}
            >
              단계별 설명
            </button>
            <button
              className={`px-4 py-2 text-sm font-medium transition-colors ${
                activeTab === 'implementation'
                  ? 'text-blue-600 border-b-2 border-blue-600 bg-white'
                  : 'text-gray-600 hover:text-blue-600'
              }`}
              onClick={() => setActiveTab('implementation')}
            >
              구현 세부사항
            </button>
          </div>

          {/* Tab Content */}
          <div className="p-4">
            {activeTab === 'overview' && (
              <div className="space-y-4">
                <div>
                  <h4 className="font-semibold text-gray-900 mb-2">주요 기능 및 목적</h4>
                  <p className="text-gray-700 leading-relaxed">{purpose}</p>
                </div>
                <div>
                  <h4 className="font-semibold text-gray-900 mb-2">데이터 흐름 및 처리 방식</h4>
                  <p className="text-gray-700 leading-relaxed">{dataFlow}</p>
                </div>
              </div>
            )}

            {activeTab === 'steps' && (
              <div className="space-y-3">
                <h4 className="font-semibold text-gray-900 mb-3">알고리즘 단계별 설명</h4>
                <div className="space-y-3">
                  {steps.map((step: AlgorithmStep, index: number) => (
                    <div key={index} className="flex space-x-3">
                      <div className="flex-shrink-0">
                        <div className="w-6 h-6 bg-blue-600 text-white rounded-full flex items-center justify-center text-sm font-medium">
                          {index + 1}
                        </div>
                      </div>
                      <div className="flex-1">
                        <h5 className="font-medium text-gray-900">{step.title}</h5>
                        <p className="text-gray-700 text-sm mt-1">{step.description}</p>
                        {step.details && (
                          <p className="text-gray-600 text-xs mt-2 italic">{step.details}</p>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {activeTab === 'implementation' && (
              <div>
                <h4 className="font-semibold text-gray-900 mb-2">핵심 로직 및 구현 세부사항</h4>
                <div className="bg-gray-50 p-3 rounded border text-sm text-gray-700 leading-relaxed">
                  {coreLogic}
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}