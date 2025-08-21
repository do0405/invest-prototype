import React, { useState } from 'react';
import { ChartBarIcon, XMarkIcon, ChevronLeftIcon, ChevronRightIcon } from '@heroicons/react/24/outline';
import TradingViewChart from './TradingViewChart';
import TableSkeleton from './TableSkeleton';

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
  striped?: boolean;
  hoverable?: boolean;
  title?: string;
  description?: string;
  // 페이지네이션 관련 props
  itemsPerPage?: number;
  showPagination?: boolean;
  paginationType?: 'numbers' | 'loadMore' | 'infinite';
  // 차트 관련 props
  showInlineChart?: boolean;
  chartHeight?: string;
  // 로딩 상태
  loading?: boolean;
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
        <div className="fixed inset-0 bg-black bg-opacity-50 transition-opacity" onClick={onClose}></div>
        
        <div className="inline-block align-bottom bg-card rounded-notion text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-6xl sm:w-full">
          <div className="bg-card px-4 pt-5 pb-4 sm:p-6 sm:pb-4">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-lg leading-6 font-medium text-foreground flex items-center gap-2">
                <ChartBarIcon className="h-6 w-6 text-accent" />
                {symbol} - TradingView Chart
              </h3>
              <button
                onClick={onClose}
                className="bg-muted hover:bg-border rounded-notion p-2 transition-colors"
              >
                <XMarkIcon className="h-5 w-5 text-muted-foreground" />
              </button>
            </div>
            <div className="w-full h-96 sm:h-[500px]">
              <TradingViewChart symbol={symbol} height="100%" />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

// 셀 값 포맷팅 함수
function formatCellValue(value: string | number | boolean | null | undefined): React.ReactNode {
  if (value === null || value === undefined || value === '' || value === 'N/A') {
    return <span className="text-muted-foreground italic">N/A</span>;
  }
  
  if (typeof value === 'number') {
    // 숫자인 경우 소수점 처리
    if (Number.isInteger(value)) {
      return value.toLocaleString();
    } else {
      return value.toFixed(2);
    }
  }
  
  if (typeof value === 'boolean') {
    return value ? 'Yes' : 'No';
  }
  
  return String(value);
}

function DataTable<T extends Record<string, string | number | boolean | null | undefined>>({
  data,
  columns,
  rowKey,
  className = '',
  headerRowClassName = '',
  responsive = true,
  cardClassName = 'bg-card rounded-notion shadow-notion border border-border p-4 mb-3 hover:shadow-notion-hover transition-shadow',
  onRowClick,
  striped = true,
  hoverable = true,
  title,
  description,
  itemsPerPage = 20,
  showPagination = true,
  paginationType = 'numbers',
  showInlineChart = false,
  chartHeight = '400px',
  loading = false
}: DataTableProps<T>) {
  const [selectedSymbol, setSelectedSymbol] = useState<string | null>(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [currentPage, setCurrentPage] = useState(1);
  const [visibleItems, setVisibleItems] = useState(itemsPerPage);

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

  // 페이지네이션 계산
  const totalPages = Math.ceil(data.length / itemsPerPage);
  const startIndex = (currentPage - 1) * itemsPerPage;
  const endIndex = startIndex + itemsPerPage;
  
  // 표시할 데이터 계산
  const getPaginatedData = () => {
    if (!showPagination) return data;
    
    switch (paginationType) {
      case 'loadMore':
        return data.slice(0, visibleItems);
      case 'infinite':
        return data.slice(0, visibleItems);
      case 'numbers':
      default:
        return data.slice(startIndex, endIndex);
    }
  };
  
  const paginatedData = getPaginatedData();

  const handleRowClick = (item: T) => {
    // symbol 또는 ticker 필드 찾기
    const symbol = item.symbol || item.ticker || item.Symbol || item.Ticker;
    if (symbol) {
      setSelectedSymbol(String(symbol));
      if (showInlineChart) {
        // 인라인 차트 표시 시에는 모달을 열지 않음
      } else {
        setIsModalOpen(true);
      }
    }
    if (onRowClick) {
      onRowClick(item);
    }
  };
  
  const handleLoadMore = () => {
    setVisibleItems(prev => Math.min(prev + itemsPerPage, data.length));
  };
  
  const handlePageChange = (page: number) => {
    setCurrentPage(page);
  };
  
  // 페이지네이션 컴포넌트
  const renderPagination = () => {
    if (!showPagination || paginationType !== 'numbers') return null;
    
    const pageNumbers = [];
    const maxVisiblePages = 5;
    let startPage = Math.max(1, currentPage - Math.floor(maxVisiblePages / 2));
    let endPage = Math.min(totalPages, startPage + maxVisiblePages - 1);
    
    if (endPage - startPage + 1 < maxVisiblePages) {
      startPage = Math.max(1, endPage - maxVisiblePages + 1);
    }
    
    for (let i = startPage; i <= endPage; i++) {
      pageNumbers.push(i);
    }
    
    return (
      <div className="flex items-center justify-between px-6 py-3 bg-gray-50 border-t border-gray-200">
        <div className="flex items-center text-sm text-gray-700">
          <span>
            Showing {startIndex + 1} to {Math.min(endIndex, data.length)} of {data.length} results
          </span>
        </div>
        <div className="flex items-center space-x-2">
          <button
            onClick={() => handlePageChange(currentPage - 1)}
            disabled={currentPage === 1}
            className="relative inline-flex items-center px-2 py-2 rounded-md border border-gray-300 bg-white text-sm font-medium text-gray-500 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            <ChevronLeftIcon className="h-5 w-5" />
          </button>
          
          {startPage > 1 && (
            <>
              <button
                onClick={() => handlePageChange(1)}
                className="relative inline-flex items-center px-3 py-2 rounded-md border border-gray-300 bg-white text-sm font-medium text-gray-700 hover:bg-gray-50"
              >
                1
              </button>
              {startPage > 2 && <span className="text-gray-500">...</span>}
            </>
          )}
          
          {pageNumbers.map(number => (
            <button
              key={number}
              onClick={() => handlePageChange(number)}
              className={`relative inline-flex items-center px-3 py-2 rounded-md border text-sm font-medium ${
                number === currentPage
                  ? 'z-10 bg-blue-50 border-blue-500 text-blue-600'
                  : 'bg-white border-gray-300 text-gray-700 hover:bg-gray-50'
              }`}
            >
              {number}
            </button>
          ))}
          
          {endPage < totalPages && (
            <>
              {endPage < totalPages - 1 && <span className="text-gray-500">...</span>}
              <button
                onClick={() => handlePageChange(totalPages)}
                className="relative inline-flex items-center px-3 py-2 rounded-md border border-gray-300 bg-white text-sm font-medium text-gray-700 hover:bg-gray-50"
              >
                {totalPages}
              </button>
            </>
          )}
          
          <button
            onClick={() => handlePageChange(currentPage + 1)}
            disabled={currentPage === totalPages}
            className="relative inline-flex items-center px-2 py-2 rounded-md border border-gray-300 bg-white text-sm font-medium text-gray-500 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            <ChevronRightIcon className="h-5 w-5" />
          </button>
        </div>
      </div>
    );
  };
  
  // Load More 버튼 컴포넌트
  const renderLoadMore = () => {
    if (!showPagination || paginationType !== 'loadMore') return null;
    if (visibleItems >= data.length) return null;
    
    return (
      <div className="flex justify-center px-6 py-4 bg-gray-50 border-t border-gray-200">
        <button
          onClick={handleLoadMore}
          className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
        >
          Load More ({data.length - visibleItems} remaining)
        </button>
      </div>
    );
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
      {/* 헤더 섹션 */}
      {(title || description) && (
        <div className="mb-6">
          {title && (
            <h2 className="text-2xl font-bold text-gray-900 mb-2">{title}</h2>
          )}
          {description && (
            <p className="text-gray-600">{description}</p>
          )}
        </div>
      )}
      
      {/* 인라인 차트 */}
      {showInlineChart && selectedSymbol && (
        <div className="mb-6 bg-white rounded-lg shadow-lg border border-gray-200 overflow-hidden">
          <div className="bg-gradient-to-r from-blue-600 to-purple-600 text-white p-4">
            <h3 className="text-lg font-semibold flex items-center gap-2">
              <ChartBarIcon className="h-6 w-6" />
              {selectedSymbol} - Live Chart
            </h3>
          </div>
          <div style={{ height: chartHeight }}>
            <TradingViewChart symbol={selectedSymbol} height={chartHeight} />
          </div>
        </div>
      )}
      
      {loading ? (
        <TableSkeleton rows={itemsPerPage} columns={columns.length} />
      ) : (
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

              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {paginatedData.map((item, idx) => (
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
                        {col.render ? col.render(item) : formatCellValue(item[col.key])}
                      </div>
                    </td>
                  ))}

                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* 모바일 카드 뷰 */}
        {responsive && (
          <div className="sm:hidden space-y-3 p-4">
            {paginatedData.map((item, idx) => (
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
                        {col.render ? col.render(item) : formatCellValue(item[col.key])}
                      </span>
                    </div>
                  ))}

                </div>
              </div>
            ))}
          </div>
        )}
        
          {/* 페이지네이션 */}
          {renderPagination()}
          {renderLoadMore()}
        </div>
      )}

      {/* TradingView 모달 */}
      {selectedSymbol && (
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
