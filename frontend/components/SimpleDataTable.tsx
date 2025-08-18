'use client';

import React from 'react';

export interface SimpleTableColumn {
  key: string;
  header: string;
  render?: (item: Record<string, unknown>) => React.ReactNode;
}

interface SimpleDataTableProps {
  data: Record<string, unknown>[];
  columns: SimpleTableColumn[];
  title?: string;
  description?: string;
  onRowClick?: (item: Record<string, unknown>) => void;
}

export default function SimpleDataTable({ data, columns, title, description, onRowClick }: SimpleDataTableProps) {
  if (!data || data.length === 0) {
    return (
      <div className="bg-white rounded-lg shadow p-6 text-center">
        <h3 className="text-lg font-semibold text-gray-800 mb-2">{title}</h3>
        {description && <p className="text-gray-600 mb-4">{description}</p>}
        <p className="text-gray-500">No data available</p>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-lg shadow overflow-hidden">
      {(title || description) && (
        <div className="bg-gradient-to-r from-blue-600 to-purple-600 text-white p-4">
          {title && <h3 className="text-lg font-semibold">{title}</h3>}
          {description && <p className="text-blue-100 text-sm mt-1">{description}</p>}
        </div>
      )}
      
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              {columns.map((column) => (
                <th
                  key={column.key}
                  className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                >
                  {column.header}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {data.map((item, index) => (
              <tr 
                key={index} 
                className={`hover:bg-gray-50 ${onRowClick ? 'cursor-pointer' : ''}`}
                onClick={() => onRowClick && onRowClick(item)}
              >
                {columns.map((column) => (
                  <td key={column.key} className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {(column.render 
                      ? column.render(item)
                      : item[column.key] || <span className="text-gray-400 italic">N/A</span>
                    ) as React.ReactNode}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      
      <div className="bg-gray-50 px-6 py-3 text-sm text-gray-500">
        Total: {data.length} items
      </div>
    </div>
  );
}