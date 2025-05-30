import React from 'react';
import Link from 'next/link';
import { FaTimes, FaHome } from 'react-icons/fa';

// Strategy 인터페이스 정의 추가
interface Strategy {
  id: string;
  name: string;
}

interface SidebarProps {
  isOpen: boolean;
  onClose: () => void;
  strategies: Strategy[];
}

const Sidebar: React.FC<SidebarProps> = ({ isOpen, onClose, strategies }) => {
  return (
    <div
      className={`fixed inset-y-0 left-0 transform ${isOpen ? 'translate-x-0' : '-translate-x-full'}
                  transition-transform duration-300 ease-in-out 
                  bg-white text-gray-800 w-64 space-y-6 py-7 px-2 z-30`}
    >
      <div className="flex items-center justify-between px-4">
        <h2 className="flex-grow text-2xl font-semibold text-center">Strategies</h2>
        <Link 
          href="/"
          className="text-gray-800 hover:text-indigo-600 p-2 rounded-md transition-all duration-200 ease-in-out hover:bg-indigo-100 active:bg-indigo-200 transform hover:scale-105 active:scale-95"
        >
          <FaHome size={24} />
        </Link>
        <button onClick={onClose} className="md:hidden text-gray-800 hover:text-gray-600 ml-2">
          <FaTimes size={24} />
        </button>
      </div>

      <nav>
        {strategies.map((strategy) => (
          <Link
            key={strategy.id}
            href={`/strategy/${strategy.id}`}
            className="block py-2.5 px-4 rounded-md transition-all duration-200 ease-in-out hover:bg-indigo-100 hover:text-indigo-700 active:bg-indigo-200 transform hover:translate-x-1 active:translate-x-0.5"
          >
            {strategy.name}
          </Link>
        ))}
      </nav>
    </div>
  );
};

export default Sidebar;