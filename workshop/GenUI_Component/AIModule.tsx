import React, { useState, useEffect } from 'react';
import { Plus, Trash2, Edit3, RefreshCw, MessageCircle, Maximize2, Minimize2, Zap, Database, Sliders, Calculator } from 'lucide-react';
import SparklesIcon from './icons/SparklesIcon';
import AIWidgetRenderer from './AIWidgetRenderer';
import { Target } from 'lucide-react';  // Add to existing lucide imports
import GoalWidgetRenderer from './GoalWidgetRenderer';
import SimulationWidgetRenderer from './SimulationWidgetRenderer';
import type { AIWidget } from '../types/aiModule';
import { API_URL } from '../apiConfig';

interface AIModuleProps {
  userId: string;
  onOpenChat: (editWidget?: AIWidget) => void;
}

const AIModule: React.FC<AIModuleProps> = ({ userId, onOpenChat }) => {
  const [widgets, setWidgets] = useState<AIWidget[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedWidget, setExpandedWidget] = useState<string | null>(null);
  const [refreshingWidgets, setRefreshingWidgets] = useState<Set<string>>(new Set());
  const [activeFilter, setActiveFilter] = useState<'all' | 'charts' | 'simulations' | 'goals'>('all');

  useEffect(() => {
    loadWidgets();
  }, [userId]);

  const loadWidgets = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await fetch(`${API_URL}/ai-widgets`, {
        headers: {
          'X-User-Id': userId,
        },
      });
      
      if (!response.ok) {
        throw new Error('Failed to load widgets');
      }
      
      const data = await response.json();
      setWidgets(data);
    } catch (err) {
      console.error('Failed to load widgets:', err);
      setError(err instanceof Error ? err.message : 'Failed to load widgets');
    } finally {
      setLoading(false);
    }
  };

  const handleRefreshWidget = async (widgetId: string) => {
    setRefreshingWidgets(prev => new Set(prev).add(widgetId));
    
    try {
      const response = await fetch(`${API_URL}/ai-widgets/${widgetId}/refresh`, {
        method: 'POST',
        headers: {
          'X-User-Id': userId,
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to refresh widget');
      }

      const data = await response.json();
      setWidgets(prev => prev.map(w => 
        w.id === widgetId ? data.widget : w
      ));
      
    } catch (err) {
      console.error('Failed to refresh widget:', err);
      alert(err instanceof Error ? err.message : 'Failed to refresh widget');
    } finally {
      setRefreshingWidgets(prev => {
        const next = new Set(prev);
        next.delete(widgetId);
        return next;
      });
    }
  };

  const handleDeleteWidget = async (widgetId: string) => {
    if (!confirm('Are you sure you want to delete this widget? Only the AI can recreate it.')) {
      return;
    }

    try {
      const response = await fetch(`${API_URL}/ai-widgets/${widgetId}`, {
        method: 'DELETE',
        headers: {
          'X-User-Id': userId,
        },
      });

      if (!response.ok) {
        throw new Error('Failed to delete widget');
      }

      setWidgets(widgets.filter(w => w.id !== widgetId));
    } catch (err) {
      console.error('Failed to delete widget:', err);
      alert('Failed to delete widget. Please try again.');
    }
  };

  const handleEditWidget = (widget: AIWidget) => {
    onOpenChat(widget);
  };

  const toggleExpand = (widgetId: string) => {
    setExpandedWidget(expandedWidget === widgetId ? null : widgetId);
  };

  const formatLastRefreshed = (dateStr: string | null) => {
    if (!dateStr) return null;
    const date = new Date(dateStr);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);
    
    if (diffMins < 1) return 'just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    if (diffHours < 24) return `${diffHours}h ago`;
    return `${diffDays}d ago`;
  };

  // Filter widgets based on active filter
  const filteredWidgets = widgets.filter(widget => {
    if (activeFilter === 'all') return true;
    if (activeFilter === 'simulations') return widget.widget_type === 'simulation';
    if (activeFilter === 'goals') return widget.widget_type === 'goal';
    if (activeFilter === 'charts') return widget.widget_type !== 'simulation' && widget.widget_type !== 'goal';
    return true;
  });

  const chartCount = widgets.filter(w => w.widget_type !== 'simulation').length;
  const simulationCount = widgets.filter(w => w.widget_type === 'simulation').length;
  const goalCount = widgets.filter(w => w.widget_type === 'goal').length;

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto mb-4"></div>
          <p className="text-gray-500">Loading your AI modules...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="bg-gradient-to-r from-indigo-900 via-purple-900 to-indigo-900 rounded-2xl p-8 relative overflow-hidden">
        {/* Animated background particles */}
        <div className="absolute inset-0 overflow-hidden">
          <div className="absolute top-4 left-10 w-2 h-2 bg-blue-400 rounded-full animate-pulse opacity-60"></div>
          <div className="absolute top-12 right-20 w-1.5 h-1.5 bg-purple-400 rounded-full animate-pulse opacity-60" style={{ animationDelay: '0.5s' }}></div>
          <div className="absolute bottom-8 left-1/4 w-2 h-2 bg-indigo-400 rounded-full animate-pulse opacity-60" style={{ animationDelay: '1s' }}></div>
          <div className="absolute top-1/3 right-1/3 w-1 h-1 bg-pink-400 rounded-full animate-pulse opacity-60" style={{ animationDelay: '1.5s' }}></div>
          <div className="absolute bottom-4 right-10 w-1.5 h-1.5 bg-cyan-400 rounded-full animate-pulse opacity-60" style={{ animationDelay: '0.7s' }}></div>
          <div className="absolute top-1/2 left-1/3 w-1 h-1 bg-yellow-400 rounded-full animate-pulse opacity-40" style={{ animationDelay: '0.3s' }}></div>
        </div>
        
        <div className="relative z-10 flex items-start justify-between">
          <div className="flex items-center gap-4">
            <div className="p-3 bg-white/10 backdrop-blur rounded-xl border border-white/20">
              <SparklesIcon size={32} />
            </div>
            <div>
              <h2 className="text-3xl font-bold text-white mb-2">AI Module</h2>
              <p className="text-indigo-200 max-w-lg">
                Your personal AI-powered dashboard. Create 
                <span className="text-cyan-300"> dynamic charts</span>, 
                <span className="text-purple-300"> static snapshots</span>, or
                <span className="text-amber-300"> interactive simulators</span> to plan your financial future.
              </p>
            </div>
          </div>
          
          <button
            onClick={() => onOpenChat()}
            className="flex items-center gap-2 px-5 py-2.5 bg-white/10 hover:bg-white/20 backdrop-blur rounded-xl text-white transition-all border border-white/20 hover:border-white/40"
          >
            <MessageCircle className="h-4 w-4" />
            <span>Ask AI to Create</span>
          </button>
        </div>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-xl flex items-center justify-between">
          <span>{error}</span>
          <button
            onClick={loadWidgets}
            className="flex items-center gap-1 text-red-600 hover:text-red-800"
          >
            <RefreshCw className="h-4 w-4" />
            Retry
          </button>
        </div>
      )}

      {/* Filter Tabs */}
      {widgets.length > 0 && (
        <div className="flex items-center gap-2 bg-white rounded-xl p-2 border border-gray-200 shadow-sm">
          <button
            onClick={() => setActiveFilter('all')}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all ${
              activeFilter === 'all' 
                ? 'bg-indigo-100 text-indigo-700' 
                : 'text-gray-600 hover:bg-gray-100'
            }`}
          >
            <SparklesIcon size={16} />
            All ({widgets.length})
          </button>
          <button
            onClick={() => setActiveFilter('charts')}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all ${
              activeFilter === 'charts' 
                ? 'bg-blue-100 text-blue-700' 
                : 'text-gray-600 hover:bg-gray-100'
            }`}
          >
            <Database className="h-4 w-4" />
            Charts ({chartCount})
          </button>
          <button
            onClick={() => setActiveFilter('simulations')}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all ${
              activeFilter === 'simulations' 
                ? 'bg-amber-100 text-amber-700' 
                : 'text-gray-600 hover:bg-gray-100'
            }`}
          >
            <Sliders className="h-4 w-4" />
            Simulators ({simulationCount})
          </button>
          <button
            onClick={() => setActiveFilter('goals')}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all ${
              activeFilter === 'goals' 
                ? 'bg-green-100 text-green-700' 
                : 'text-gray-600 hover:bg-gray-100'
            }`}
          >
            <Target className="h-4 w-4" />
            Goals ({goalCount})
          </button>
        </div>
      )}

      {/* Empty State */}
      {widgets.length === 0 && !error && (
        <div className="bg-white rounded-2xl border-2 border-dashed border-gray-200 p-12 hover:border-indigo-300 transition-colors">
          <div className="text-center max-w-md mx-auto">
            <div className="w-20 h-20 bg-gradient-to-br from-indigo-100 to-purple-100 rounded-2xl flex items-center justify-center mx-auto mb-6 shadow-lg">
              <SparklesIcon size={40} />
            </div>
            <h3 className="text-xl font-semibold text-gray-900 mb-3">
              No AI modules yet
            </h3>
            <p className="text-gray-500 mb-6">
              Start a conversation with the AI assistant to create your first visualization or simulator.
            </p>
            
            {/* Two Column Examples */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-left mb-6">
              {/* Charts Column */}
              <div className="bg-gradient-to-r from-blue-50 to-cyan-50 rounded-xl p-4 border border-blue-100">
                <div className="flex items-center gap-2 mb-3">
                  <Database className="h-5 w-5 text-blue-600" />
                  <span className="font-medium text-blue-900">Charts</span>
                </div>
                <div className="space-y-2">
                  <p className="text-xs text-blue-700 flex items-start gap-2">
                    <Zap className="h-3 w-3 mt-0.5 flex-shrink-0" />
                    <span>"Create a spending by category chart"</span>
                  </p>
                  <p className="text-xs text-blue-700 flex items-start gap-2">
                    <Zap className="h-3 w-3 mt-0.5 flex-shrink-0" />
                    <span>"Show monthly income vs expenses"</span>
                  </p>
                </div>
              </div>
              
              {/* Simulators Column */}
              <div className="bg-gradient-to-r from-amber-50 to-orange-50 rounded-xl p-4 border border-amber-100">
                <div className="flex items-center gap-2 mb-3">
                  <Sliders className="h-5 w-5 text-amber-600" />
                  <span className="font-medium text-amber-900">Simulators</span>
                </div>
                <div className="space-y-2">
                  <p className="text-xs text-amber-700 flex items-start gap-2">
                    <Calculator className="h-3 w-3 mt-0.5 flex-shrink-0" />
                    <span>"Create a loan repayment simulator"</span>
                  </p>
                  <p className="text-xs text-amber-700 flex items-start gap-2">
                    <Calculator className="h-3 w-3 mt-0.5 flex-shrink-0" />
                    <span>"Build a retirement calculator"</span>
                  </p>
                </div>
              </div>
            </div>
            
            <button
              onClick={() => onOpenChat()}
              className="inline-flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-indigo-600 to-purple-600 text-white rounded-xl hover:from-indigo-700 hover:to-purple-700 transition-all shadow-lg shadow-indigo-200 hover:shadow-xl hover:shadow-indigo-300"
            >
              <MessageCircle className="h-5 w-5" />
              Open AI Chat
            </button>
          </div>
        </div>
      )}

      {/* Widgets Grid */}
      {filteredWidgets.length > 0 && (
        <div className={`grid gap-6 ${expandedWidget ? 'grid-cols-1' : 'grid-cols-1 lg:grid-cols-2'}`}>
          {filteredWidgets
            .filter(widget => !expandedWidget || widget.id === expandedWidget)
            .map((widget) => {
              const isDynamic = widget.data_mode === 'dynamic';
              const isSimulation = widget.widget_type === 'simulation';
              const isRefreshing = refreshingWidgets.has(widget.id);
              const isGoal = widget.widget_type === 'goal';
              
              return (
                <div
                  key={widget.id}
                  className={`bg-white rounded-xl shadow-sm border overflow-hidden transition-all hover:shadow-md ${
                  expandedWidget === widget.id ? 'col-span-full' : ''
                } ${isSimulation ? 'border-amber-200' : isGoal ? 'border-green-200' : 'border-gray-200'}`}
                >
                  {/* Widget Header */}
                  <div className={`px-6 py-4 border-b flex items-center justify-between ${
                    isSimulation 
                      ? 'bg-gradient-to-r from-amber-50 to-orange-50 border-amber-100' 
                      : isGoal
                        ? 'bg-gradient-to-r from-green-50 to-emerald-50 border-green-100'
                        : 'bg-gradient-to-r from-gray-50 to-white border-gray-100'
                  }`}>
                    <div className="flex items-center gap-3">
                      <div className={`p-2 rounded-lg ${
                        isSimulation 
                          ? 'bg-gradient-to-br from-amber-100 to-orange-100' 
                          : 'bg-gradient-to-br from-indigo-100 to-purple-100'
                      }`}>
                        {isSimulation ? (
                          <Sliders className="h-4 w-4 text-amber-600" />
                        ) : isGoal ? (
                          <Target className="h-4 w-4 text-green-600" />
                        ) : (
                          <SparklesIcon size={16} />
                        )}
                        {isSimulation ? (
                          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-amber-50 text-amber-700 border border-amber-200">
                            <Sliders className="h-3 w-3" />
                            Interactive
                          </span>
                        ) : isGoal ? (
                          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-green-50 text-green-700 border border-green-200">
                            <Target className="h-3 w-3" />
                            Goal
                          </span>
                        ) : isDynamic ? (
                          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-cyan-50 text-cyan-700 border border-cyan-200">
                            <Zap className="h-3 w-3" />
                            Live
                          </span>
                        ) : null}
                      </div>
                      <div>
                        <div className="flex items-center gap-2">
                          <h3 className="font-semibold text-gray-900">{widget.title}</h3>
                          {/* Widget type indicator */}
                          {isSimulation ? (
                            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-amber-50 text-amber-700 border border-amber-200">
                              <Sliders className="h-3 w-3" />
                              Interactive
                            </span>
                          ) : isDynamic ? (
                            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-cyan-50 text-cyan-700 border border-cyan-200">
                              <Zap className="h-3 w-3" />
                              Live
                            </span>
                          ) : (
                            <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-600 border border-gray-200">
                              <Database className="h-3 w-3" />
                              Static
                            </span>
                          )}
                        </div>
                        {widget.description && (
                          <p className="text-xs text-gray-500 mt-0.5">{widget.description}</p>
                        )}
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      {/* Last refreshed indicator for dynamic widgets */}
                      {isDynamic && !isSimulation && widget.last_refreshed && (
                        <span className="text-xs text-gray-400 mr-2 hidden sm:inline">
                          Updated {formatLastRefreshed(widget.last_refreshed)}
                        </span>
                      )}
                      {!isDynamic && !isSimulation && (
                        <span className="text-xs text-gray-400 mr-2 hidden sm:inline">
                          {new Date(widget.updated_at).toLocaleDateString()}
                        </span>
                      )}
                      
                      {/* Refresh button for dynamic widgets (not simulations) */}
                      {(isDynamic || isGoal) && !isSimulation && (
                        <button
                          onClick={() => handleRefreshWidget(widget.id)}
                          disabled={isRefreshing}
                          className={`p-2 rounded-lg transition-colors ${
                            isRefreshing 
                              ? 'text-cyan-400 bg-cyan-50' 
                              : 'text-cyan-600 hover:text-cyan-700 hover:bg-cyan-50'
                          }`}
                          title="Refresh data"
                        >
                          <RefreshCw className={`h-4 w-4 ${isRefreshing ? 'animate-spin' : ''}`} />
                        </button>
                      )}
                      
                      <button
                        onClick={() => toggleExpand(widget.id)}
                        className="p-2 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg transition-colors"
                        title={expandedWidget === widget.id ? 'Minimize' : 'Expand'}
                      >
                        {expandedWidget === widget.id ? (
                          <Minimize2 className="h-4 w-4" />
                        ) : (
                          <Maximize2 className="h-4 w-4" />
                        )}
                      </button>
                      <button
                        onClick={() => handleEditWidget(widget)}
                        className="p-2 text-gray-400 hover:text-indigo-600 hover:bg-indigo-50 rounded-lg transition-colors"
                        title="Edit with AI"
                      >
                        <Edit3 className="h-4 w-4" />
                      </button>
                      <button
                        onClick={() => handleDeleteWidget(widget.id)}
                        className="p-2 text-gray-400 hover:text-red-600 hover:bg-red-50 rounded-lg transition-colors"
                        title="Delete"
                      >
                        <Trash2 className="h-4 w-4" />
                      </button>
                    </div>
                  </div>

                  {/* Widget Content */}
                  <div className={`p-6 relative ${
                    expandedWidget === widget.id 
                      ? 'h-[600px]' 
                      : isSimulation 
                        ? 'h-[480px]' 
                        : isGoal
                          ? 'h-[320px]'
                          : 'h-80'
                  }`}>

                  // Update renderer routing
                  {isSimulation ? (
                    <SimulationWidgetRenderer widget={widget} />
                  ) : isGoal ? (
                    <GoalWidgetRenderer widget={widget} />
                  ) : (
                    <AIWidgetRenderer 
                      widget={widget} 
                      data={widget.config.customProps?.data}
                    />
                  )}
                  </div>
                </div>
              );
            })}
        </div>
      )}

      {/* Quick Actions */}
      {widgets.length > 0 && (
        <div className="bg-gradient-to-r from-gray-50 to-indigo-50 rounded-xl p-6 border border-gray-200">
          <div className="flex items-center justify-between flex-wrap gap-4">
            <div>
              <h4 className="font-medium text-gray-900 mb-1">Want to add more modules?</h4>
              <p className="text-sm text-gray-500">
                Chat with the AI to create <span className="text-cyan-600 font-medium">charts</span> or 
                <span className="text-amber-600 font-medium"> interactive simulators</span>!
              </p>
            </div>
            <div className="flex gap-3">
              <button
                onClick={loadWidgets}
                className="flex items-center gap-2 px-4 py-2 bg-white border border-gray-200 text-gray-700 rounded-lg hover:bg-gray-50 transition-colors shadow-sm"
              >
                <RefreshCw className="h-4 w-4" />
                Reload All
              </button>
              <button
                onClick={() => onOpenChat()}
                className="flex items-center gap-2 px-4 py-2 bg-gradient-to-r from-indigo-600 to-purple-600 text-white rounded-lg hover:from-indigo-700 hover:to-purple-700 transition-colors shadow-sm"
              >
                <Plus className="h-4 w-4" />
                Create New
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default AIModule;