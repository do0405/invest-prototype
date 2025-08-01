'use client';

import React, { useState, useEffect } from 'react';
import { XMarkIcon } from '@heroicons/react/24/outline';

export interface NumberFilter {
  key: string;
  min: number;
  max: number;
  value: [number, number];
  step: number;
}

interface NumberInputFilterProps {
  filters: NumberFilter[];
  onFilterChange: (filterKey: string, newValue: [number, number]) => void;
  onResetFilters: () => void;
  showFilters: boolean;
  onToggleFilters: () => void;
}

const NumberInputFilter: React.FC<NumberInputFilterProps> = ({
  filters,
  onFilterChange,
  onResetFilters,
  showFilters,
  onToggleFilters
}) => {
  const [localFilters, setLocalFilters] = useState<{ [key: string]: [string, string] }>({});

  // 초기화 시 필터 값을 문자열로 변환
  useEffect(() => {
    const initialLocalFilters: { [key: string]: [string, string] } = {};
    filters.forEach(filter => {
      initialLocalFilters[filter.key] = [
        filter.value[0].toString(),
        filter.value[1].toString()
      ];
    });
    setLocalFilters(initialLocalFilters);
  }, [filters]);

  const handleInputChange = (filterKey: string, index: 0 | 1, value: string) => {
    setLocalFilters(prev => {
      const current = prev[filterKey] || ['', ''];
      const newValue: [string, string] = [...current] as [string, string];
      newValue[index] = value;
      return {
        ...prev,
        [filterKey]: newValue
      };
    });
  };

  const handleInputBlur = (filterKey: string) => {
    const localValue = localFilters[filterKey];
    if (!localValue) return;

    const filter = filters.find(f => f.key === filterKey);
    if (!filter) return;

    let minVal = parseFloat(localValue[0]) || filter.min;
    let maxVal = parseFloat(localValue[1]) || filter.max;

    // 범위 검증
    minVal = Math.max(filter.min, Math.min(filter.max, minVal));
    maxVal = Math.max(filter.min, Math.min(filter.max, maxVal));

    // min이 max보다 큰 경우 교정
    if (minVal > maxVal) {
      [minVal, maxVal] = [maxVal, minVal];
    }

    // 로컬 상태 업데이트
    setLocalFilters(prev => ({
      ...prev,
      [filterKey]: [minVal.toString(), maxVal.toString()]
    }));

    // 부모 컴포넌트에 변경사항 전달
    onFilterChange(filterKey, [minVal, maxVal]);
  };

  const handleReset = () => {
    // 모든 필터를 초기값으로 리셋
    const resetLocalFilters: { [key: string]: [string, string] } = {};
    filters.forEach(filter => {
      resetLocalFilters[filter.key] = [
        filter.min.toString(),
        filter.max.toString()
      ];
    });
    setLocalFilters(resetLocalFilters);
    onResetFilters();
  };

  const formatLabel = (key: string) => {
    return key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
  };

  const hasActiveFilters = filters.some(filter => 
    filter.value[0] !== filter.min || filter.value[1] !== filter.max
  );

  return (
    <div className="mb-6">
      {/* 필터 토글 버튼 */}
      <div className="flex justify-between items-center mb-4">
        <button
          onClick={onToggleFilters}
          className="px-6 py-3 bg-gradient-to-r from-blue-600 to-purple-600 text-white rounded-lg hover:from-blue-700 hover:to-purple-700 transition-all duration-300 shadow-lg hover:shadow-xl transform hover:scale-105 flex items-center gap-2"
        >
          <span className="text-lg">🎛️</span>
          {showFilters ? 'Hide Filters' : 'Show Filters'}
          {hasActiveFilters && (
            <span className="bg-red-500 text-white text-xs px-2 py-1 rounded-full ml-2">
              {filters.filter(f => f.value[0] !== f.min || f.value[1] !== f.max).length}
            </span>
          )}
          <span className={`transform transition-transform duration-300 ${showFilters ? 'rotate-180' : ''}`}>
            ▼
          </span>
        </button>
        
        {hasActiveFilters && (
          <button
            onClick={handleReset}
            className="px-4 py-2 bg-red-500 text-white rounded-lg hover:bg-red-600 transition-colors flex items-center gap-2"
          >
            <XMarkIcon className="h-4 w-4" />
            Clear All Filters
          </button>
        )}
      </div>

      {/* 필터 패널 */}
      <div className={`overflow-hidden transition-all duration-500 ease-in-out ${
        showFilters 
          ? 'max-h-96 opacity-100 transform translate-y-0' 
          : 'max-h-0 opacity-0 transform -translate-y-4'
      }`}>
        <div className="bg-gradient-to-br from-white to-gray-50 rounded-xl shadow-lg p-6 border border-gray-200">
          <div className="flex justify-between items-center mb-6">
            <h3 className="text-lg font-semibold text-gray-800 flex items-center gap-2">
              <span>🎯</span>
              Number Input Filter Controls
            </h3>
            <div className="flex items-center gap-2 text-sm text-gray-600">
              <span>Active Filters:</span>
              <span className="bg-blue-100 text-blue-800 px-2 py-1 rounded-full font-medium">
                {filters.filter(f => f.value[0] !== f.min || f.value[1] !== f.max).length}
              </span>
            </div>
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {filters.map((filter) => {
              const localValue = localFilters[filter.key] || [filter.value[0].toString(), filter.value[1].toString()];
              const isActive = filter.value[0] !== filter.min || filter.value[1] !== filter.max;
              
              return (
                <div key={filter.key} className={`space-y-3 p-4 rounded-lg border-2 transition-all ${
                  isActive 
                    ? 'border-blue-300 bg-blue-50' 
                    : 'border-gray-200 bg-white'
                }`}>
                  <div className="flex items-center justify-between">
                    <label className="block text-sm font-medium text-gray-700">
                      {formatLabel(filter.key)}
                    </label>
                    {isActive && (
                      <button
                        onClick={() => {
                          setLocalFilters(prev => ({
                            ...prev,
                            [filter.key]: [filter.min.toString(), filter.max.toString()]
                          }));
                          onFilterChange(filter.key, [filter.min, filter.max]);
                        }}
                        className="text-red-500 hover:text-red-700 transition-colors"
                        title="Reset this filter"
                      >
                        <XMarkIcon className="h-4 w-4" />
                      </button>
                    )}
                  </div>
                  
                  <div className="space-y-3">
                    {/* Min Value Input */}
                    <div>
                      <label className="block text-xs text-gray-500 mb-1">Minimum Value</label>
                      <input
                        type="number"
                        value={localValue[0]}
                        onChange={(e) => handleInputChange(filter.key, 0, e.target.value)}
                        onBlur={() => handleInputBlur(filter.key)}
                        min={filter.min}
                        max={filter.max}
                        step={filter.step}
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent text-sm"
                        placeholder={`Min: ${filter.min}`}
                      />
                    </div>
                    
                    {/* Max Value Input */}
                    <div>
                      <label className="block text-xs text-gray-500 mb-1">Maximum Value</label>
                      <input
                        type="number"
                        value={localValue[1]}
                        onChange={(e) => handleInputChange(filter.key, 1, e.target.value)}
                        onBlur={() => handleInputBlur(filter.key)}
                        min={filter.min}
                        max={filter.max}
                        step={filter.step}
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent text-sm"
                        placeholder={`Max: ${filter.max}`}
                      />
                    </div>
                    
                    {/* Range Display */}
                    <div className="flex justify-between text-xs text-gray-400 pt-2 border-t border-gray-200">
                      <span>Range: {filter.min.toFixed(2)} - {filter.max.toFixed(2)}</span>
                      <span>Current: {filter.value[0].toFixed(2)} - {filter.value[1].toFixed(2)}</span>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
          
          {filters.length === 0 && (
            <div className="text-center py-8 text-gray-500">
              <span className="text-4xl mb-2 block">📊</span>
              <p>No numeric filters available for current data</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default NumberInputFilter;