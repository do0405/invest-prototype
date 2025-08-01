'use client';

import React from 'react';

interface TableSkeletonProps {
  rows?: number;
  columns?: number;
  showHeader?: boolean;
}

const TableSkeleton: React.FC<TableSkeletonProps> = ({ 
  rows = 5, 
  columns = 4, 
  showHeader = true 
}) => {
  return (
    <div className="overflow-hidden rounded-xl border border-gray-200 shadow-lg animate-pulse">
      <div className="overflow-x-auto">
        <table className="min-w-full">
          {showHeader && (
            <thead>
              <tr className="bg-gradient-to-r from-gray-50 to-gray-100">
                {Array.from({ length: columns }).map((_, colIndex) => (
                  <th key={colIndex} className="px-6 py-4">
                    <div className="h-4 bg-gray-300 rounded w-20"></div>
                  </th>
                ))}
                <th className="px-6 py-4">
                  <div className="h-4 bg-gray-300 rounded w-16"></div>
                </th>
              </tr>
            </thead>
          )}
          <tbody className="divide-y divide-gray-100">
            {Array.from({ length: rows }).map((_, rowIndex) => (
              <tr key={rowIndex} className="bg-white">
                {Array.from({ length: columns }).map((_, colIndex) => (
                  <td key={colIndex} className="px-6 py-4">
                    <div className="h-4 bg-gray-200 rounded w-full"></div>
                  </td>
                ))}
                <td className="px-6 py-4 text-center">
                  <div className="inline-flex items-center px-3 py-1.5 bg-gray-200 rounded-full">
                    <div className="h-4 w-4 bg-gray-300 rounded mr-1"></div>
                    <div className="h-3 bg-gray-300 rounded w-10"></div>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      
      {/* 페이지네이션 스켈레톤 */}
      <div className="flex items-center justify-between px-6 py-3 bg-gray-50 border-t border-gray-200">
        <div className="h-4 bg-gray-300 rounded w-48"></div>
        <div className="flex items-center space-x-2">
          {Array.from({ length: 5 }).map((_, index) => (
            <div key={index} className="h-8 w-8 bg-gray-300 rounded"></div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default TableSkeleton;