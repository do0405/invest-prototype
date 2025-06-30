import React, { useState } from 'react';
import { ChartBarIcon, XMarkIcon } from '@heroicons/react/24/outline';

export interface DataTableColumn<T> {
  key: string;
  header: React.ReactNode;
  align?: 'left' | 'right' | 'center';
  render?: (item: T) => React.ReactNode;
  sortable?: boolean;
}

interface DataTableProps<T> {
  data: T[];
  columns: DataTableColumn<T>[];
  rowKey?: (item: T, index: number) => React.Key;
  className?: string;
  headerRowClassName?: string;
  responsive?: boolean;
  cardClassName?: string;
  onRowClick?: (item: T) => void;
  onSymbolSelect?: (symbol: string) => void;
  disableModal?: boolean;
  striped?: boolean;
  hoverable?: boolean;
}

// TradingView 차트 모달 컴포넌트
interface TradingViewModalProps {
  symbol: string;
  isOpen: boolean;
  onClose: () => void;
}

function TradingViewModal({ symbol, isOpen, onClose }: TradingViewModalProps) {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 overflow-y-auto">
      <div className="flex items-center justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
        <div className="fixed inset-0 bg-gray-500 bg-opacity-75 transition-opacity" onClick={onClose}></div>
        
        <div className="inline-block align-bottom bg-white rounded-lg text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-6xl sm:w-full">
          <div className="bg-white px-4 pt-5 pb-4 sm:p-6 sm:pb-4">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-lg leading-6 font-medium text-gray-900 flex items-center gap-2">
                <ChartBarIcon className="h-6 w-6 text-blue-600" />
                {symbol} - TradingView Chart
              </h3>
              <button
                onClick={onClose}
                className="bg-gray-100 hover:bg-gray-200 rounded-full p-2 transition-colors"
              >
                <XMarkIcon className="h-5 w-5 text-gray-600" />
              </button>
            </div>
            <div className="w-full h-96 sm:h-[500px]">
              <iframe
                src={`https://www.tradingview.com/widgetembed/?frameElementId=tradingview_chart&symbol=NASDAQ:${symbol}&interval=D&hidesidetoolbar=1&hidetoptoolbar=1&symboledit=1&saveimage=1&toolbarbg=F1F3F6&studies=[]&hideideas=1&theme=Light&style=1&timezone=Etc%2FUTC&studies_overrides={}&overrides={}&enabled_features=[]&disabled_features=[]&locale=en&utm_source=localhost&utm_medium=widget&utm_campaign=chart&utm_term=${symbol}`}
                className="w-full h-full border-0 rounded-lg"
                allowTransparency={true}
                scrolling="no"
                allowFullScreen={true}
                frameBorder="0"
              ></iframe>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function DataTable<T extends Record<string, string | number | boolean | null | undefined>>({
  data,
  columns,
  rowKey,
  className = '',
  headerRowClassName = '',
  responsive = true,
  cardClassName = 'bg-white rounded-lg shadow-sm border border-gray-200 p-4 mb-3 hover:shadow-md transition-shadow',
  onRowClick,
  onSymbolSelect,
  disableModal = false,
  striped = true,
  hoverable = true
}: DataTableProps<T>) {
  const [selectedSymbol, setSelectedSymbol] = useState<string | null>(null);
  const [isModalOpen, setIsModalOpen] = useState(false);

  const getAlignClass = (align?: string) => {
    switch (align) {
      case 'right':
        return 'text-right';
      case 'center':
        return 'text-center';
      default:
        return 'text-left';
    }
  };

  const handleRowClick = (item: T) => {
    // symbol 또는 ticker 필드 찾기
    const symbol = item.symbol || item.ticker || item.Symbol || item.Ticker;
    if (symbol) {
      if (onSymbolSelect) {
        onSymbolSelect(String(symbol));
      }
      if (!disableModal) {
        setSelectedSymbol(String(symbol));
        setIsModalOpen(true);
      }
    }
    if (onRowClick) {
      onRowClick(item);
    }
  };

  const getRowClassName = (idx: number) => {
    let baseClass = 'transition-all duration-200 border-b border-gray-100';
    
    if (hoverable) {
      baseClass += ' hover:bg-gradient-to-r hover:from-blue-50 hover:to-purple-50 hover:shadow-sm cursor-pointer';
    }
    
    if (striped && idx % 2 === 0) {
      baseClass += ' bg-gray-50/50';
    } else {
      baseClass += ' bg-white';
    }
    
    return baseClass;
  };

  return (
    <>
      <div className={`overflow-hidden rounded-xl border border-gray-200 shadow-lg ${className}`}>
        {/* 데스크톱 테이블 */}
        <div className="overflow-x-auto">
          <table className="min-w-full hidden sm:table">
            <thead>
              <tr className={`bg-gradient-to-r from-gray-50 to-gray-100 ${headerRowClassName}`}>
                {columns.map(col => (
                  <th
                    key={col.key}
                    className={`px-6 py-4 text-xs font-semibold text-gray-700 uppercase tracking-wider border-b-2 border-gray-200 ${getAlignClass(col.align)}`}
                  >
                    <div className="flex items-center gap-2">
                      {col.header}
                      {col.sortable && (
                        <svg className="w-4 h-4 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16V4m0 0L3 8m4-4l4 4m6 0v12m0 0l4-4m-4 4l-4-4" />
                        </svg>
                      )}
                    </div>
                  </th>
                ))}
                <th className="px-6 py-4 text-xs font-semibold text-gray-700 uppercase tracking-wider border-b-2 border-gray-200 text-center">
                  Chart
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {data.map((item, idx) => (
                <tr
                  key={rowKey ? rowKey(item, idx) : idx}
                  className={getRowClassName(idx)}
                  onClick={() => handleRowClick(item)}
                >
                  {columns.map(col => (
                    <td
                      key={col.key}
                      className={`px-6 py-4 text-sm text-gray-900 ${getAlignClass(col.align)}`}
                    >
                      <div className="font-medium">
                        {col.render ? col.render(item) : String(item[col.key] ?? 'N/A')}
                      </div>
                    </td>
                  ))}
                  <td className="px-6 py-4 text-center">
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        handleRowClick(item);
                      }}
                      className="inline-flex items-center px-3 py-1.5 border border-transparent text-xs font-medium rounded-full text-blue-700 bg-blue-100 hover:bg-blue-200 transition-colors focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
                    >
                      <ChartBarIcon className="h-4 w-4 mr-1" />
                      Chart
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* 모바일 카드 뷰 */}
        {responsive && (
          <div className="sm:hidden space-y-3 p-4">
            {data.map((item, idx) => (
              <div
                key={rowKey ? rowKey(item, idx) : idx}
                className={cardClassName}
                onClick={() => handleRowClick(item)}
              >
                <div className="space-y-2">
                  {columns.map(col => (
                    <div key={col.key} className="flex justify-between items-center py-1">
                      <span className="text-sm font-medium text-gray-600 mr-4">{col.header}</span>
                      <span className={`text-sm font-semibold text-gray-900 ${getAlignClass(col.align)}`}>
                        {col.render ? col.render(item) : String(item[col.key] ?? 'N/A')}
                      </span>
                    </div>
                  ))}
                  <div className="pt-2 border-t border-gray-200">
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        handleRowClick(item);
                      }}
                      className="w-full inline-flex items-center justify-center px-4 py-2 border border-transparent text-sm font-medium rounded-lg text-blue-700 bg-blue-100 hover:bg-blue-200 transition-colors"
                    >
                      <ChartBarIcon className="h-4 w-4 mr-2" />
                      View Chart
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* TradingView 모달 */}
      {!disableModal && selectedSymbol && (
        <TradingViewModal
          symbol={selectedSymbol}
          isOpen={isModalOpen}
          onClose={() => {
            setIsModalOpen(false);
            setSelectedSymbol(null);
          }}
        />
      )}
    </>
  );
}

export default DataTable;
