'use client';

import React, { ReactNode } from 'react';
import { CalendarIcon, TrophyIcon, ChartBarIcon } from '@heroicons/react/24/outline';

interface EnhancedDataTableProps {
  data: Array<Record<string, unknown>>;
  onRowClick?: (symbol: string) => void;
  showChart?: boolean;
}

const EnhancedDataTable: React.FC<EnhancedDataTableProps> = ({ 
  data, 
  onRowClick,
  showChart = true 
}) => {
  if (!data || data.length === 0) {
    return (
      <div className="text-center py-8 text-gray-500">
        데이터가 없습니다.
      </div>
    );
  }

  const handleRowClick = (item: Record<string, unknown>) => {
    if (onRowClick && item.symbol) {
      onRowClick(String(item.symbol));
    }
  };

  const formatDate = (dateValue: unknown): string => {
    if (!dateValue) return 'N/A';
    try {
      const date = new Date(dateValue as string);
      return date.toLocaleDateString('ko-KR');
    } catch {
      return String(dateValue);
    }
  };

  const formatNumber = (value: unknown): string => {
    if (value === null || value === undefined) return 'N/A';
    const num = Number(value);
    if (isNaN(num)) return String(value);
    return num.toFixed(2);
  };

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full divide-y divide-gray-200">
        <thead className="bg-gray-50">
          <tr>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
              티커
            </th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
              <div className="flex items-center gap-1">
                <TrophyIcon className="h-4 w-4" />
                RS 점수
              </div>
            </th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
              <div className="flex items-center gap-1">
                <CalendarIcon className="h-4 w-4" />
                시그널 날짜
              </div>
            </th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
              충족 조건
            </th>
            {showChart && (
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                <div className="flex items-center gap-1">
                  <ChartBarIcon className="h-4 w-4" />
                  차트
                </div>
              </th>
            )}
          </tr>
        </thead>
        <tbody className="bg-white divide-y divide-gray-200">
          {data.map((item, index) => (
            <tr 
              key={`${item.symbol}-${index}`}
              className={`hover:bg-gray-50 transition-colors duration-200 ${
                onRowClick ? 'cursor-pointer' : ''
              }`}
              onClick={() => handleRowClick(item)}
            >
              <td className="px-6 py-4 whitespace-nowrap">
                <div className="flex items-center">
                  <div className="text-sm font-medium text-purple-600">
                    {String(item.symbol || 'N/A')}
                  </div>
                </div>
              </td>
              <td className="px-6 py-4 whitespace-nowrap">
                <div className="flex items-center">
                  {item.rs_score ? (
                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                      Number(item.rs_score) >= 90 
                        ? 'bg-green-100 text-green-800'
                        : Number(item.rs_score) >= 85
                        ? 'bg-blue-100 text-blue-800'
                        : 'bg-yellow-100 text-yellow-800'
                    }`}>
                      {formatNumber(item.rs_score)}
                    </span>
                  ) : (
                    <span className="text-gray-400 text-sm">N/A</span>
                  )}
                </div>
              </td>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                {formatDate(item.signal_date)}
              </td>
              <td className="px-6 py-4 whitespace-nowrap">
                {item.met_count ? (
                  <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                    Number(item.met_count) === 8
                      ? 'bg-green-100 text-green-800'
                      : Number(item.met_count) >= 6
                      ? 'bg-blue-100 text-blue-800'
                      : 'bg-gray-100 text-gray-800'
                  }`}>
                    {String(item.met_count)}/8
                  </span>
                ) : (
                  <span className="text-gray-400 text-sm">N/A</span>
                )}
              </td>
              {showChart && (
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      if (item.symbol) {
                        onRowClick?.(String(item.symbol));
                      }
                    }}
                    className="text-purple-600 hover:text-purple-900 transition-colors duration-200"
                  >
                    <ChartBarIcon className="h-5 w-5" />
                  </button>
                </td>
              )}
            </tr>
          ))}
        </tbody>
      </table>
      
      {data.length > 0 && (
        <div className="px-6 py-3 bg-gray-50 border-t border-gray-200">
          <p className="text-sm text-gray-700">
            총 <span className="font-medium">{data.length}</span>개 종목 표시
            {onRowClick && showChart && (
              <span className="text-gray-500 ml-2">
                • 행을 클릭하거나 차트 아이콘을 클릭하여 TradingView 차트를 확인하세요
              </span>
            )}
          </p>
        </div>
      )}
    </div>
  );
};

export default EnhancedDataTable;