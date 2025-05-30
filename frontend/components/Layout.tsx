import React from 'react';

interface LayoutProps {
  children: React.ReactNode;
}

const Layout: React.FC<LayoutProps> = ({ children }) => {
  return (
    <div className="flex h-screen bg-gray-100">
      {/* Sidebar */}
      <aside className="w-64 bg-white p-4 shadow-md">
        <h2 className="text-xl font-semibold mb-4">Strategies</h2>
        {/* Navigation items will go here */}
        <ul>
          <li className="mb-2"><a href="#" className="text-gray-700 hover:text-blue-500">Strategy 1</a></li>
          <li className="mb-2"><a href="#" className="text-gray-700 hover:text-blue-500">Strategy 2</a></li>
          {/* Add more strategy links as needed */}
        </ul>
      </aside>

      {/* Content Area */}
      <main className="flex-1 p-6">
        {children}
      </main>
    </div>
  );
};

export default Layout;