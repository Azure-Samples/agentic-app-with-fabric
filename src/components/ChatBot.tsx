import React, { useState, useEffect, useRef } from 'react';
import { Send, Bot, User, Loader, Zap, Database, BarChart3, PieChart, TrendingUp, X, Edit3, Sliders, Calculator, Target, PiggyBank, Home, ChevronDown, Plus, History } from 'lucide-react';
import { API_URL } from '../apiConfig';
import { AnalyticsAPI } from '../services/analyticsApi';
import type { AIWidget } from '../types/aiModule';
import type { ChatSession } from '../types/analytics';

interface ChatBotProps {
  userId: string;
  activeTab?: string;
  editingWidget?: AIWidget | null;
  onClearEditingWidget?: () => void;
}

interface Message {
  id: string;
  content: string;
  role: 'user' | 'assistant';
  timestamp: Date;
  widgetCreated?: boolean;
  widgetUpdated?: boolean;
  widgetMode?: 'static' | 'dynamic';
  widgetType?: 'chart' | 'simulation';
  simulationType?: string;
}

const WELCOME_MESSAGE: Message = {
  id: 'welcome',
  content: `Hi! I'm your banking assistant. I can help you with:\n\n• Check account balances and transactions\n• Transfer money between accounts\n• Answer questions about your finances\n• Create data charts 📊 (dynamic or static)\n• Build interactive simulators 🎛️ (What-If calculators)\n\nHow can I help you today?`,
  role: 'assistant',
  timestamp: new Date(),
};

const ChatBot: React.FC<ChatBotProps> = ({ userId, activeTab, editingWidget, onClearEditingWidget }) => {
  const [messages, setMessages] = useState<Message[]>([WELCOME_MESSAGE]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  // Session picker state
  const [sessions, setSessions] = useState<ChatSession[]>([]);
  const [isSessionDropdownOpen, setIsSessionDropdownOpen] = useState(false);
  const [isLoadingSessions, setIsLoadingSessions] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // ----- Session lifecycle -----

  /** Create a brand-new session id and reset chat UI */
  const startNewSession = () => {
    const newId = `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    localStorage.setItem('chatSessionId', newId);
    setSessionId(newId);
    setMessages([{ ...WELCOME_MESSAGE, id: 'welcome', timestamp: new Date() }]);
    setIsSessionDropdownOpen(false);
  };

  /** Load messages for an existing session from Cosmos DB */
  const switchToSession = async (session: ChatSession) => {
    const sid = session.id || session.session_id || '';
    setIsSessionDropdownOpen(false);
    setIsLoading(true);

    try {
      const history = await AnalyticsAPI.getChatHistory(sid, userId);

      // Convert Cosmos messages → UI Message objects
      const restored: Message[] = history.map((m, idx) => ({
        id: `restored_${idx}_${Date.now()}`,
        content: m.content,
        role: m.type === 'human' ? 'user' as const : 'assistant' as const,
        timestamp: m.timestamp ? new Date(m.timestamp) : new Date(),
      }));

      localStorage.setItem('chatSessionId', sid);
      setSessionId(sid);
      setMessages(restored.length > 0 ? restored : [{ ...WELCOME_MESSAGE, id: 'welcome', timestamp: new Date() }]);
    } catch (err) {
      console.error('Failed to load session history:', err);
    } finally {
      setIsLoading(false);
    }
  };

  /** Fetch the list of sessions for the current user */
  const loadSessions = async () => {
    setIsLoadingSessions(true);
    try {
      const data = await AnalyticsAPI.getChatSessionsByUser(userId);
      setSessions(data);
    } catch (err) {
      console.error('Failed to load sessions:', err);
    } finally {
      setIsLoadingSessions(false);
    }
  };

  // ----- Init: restore or create session on mount -----
  useEffect(() => {
    let storedSessionId = localStorage.getItem('chatSessionId');
    if (!storedSessionId) {
      storedSessionId = `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      localStorage.setItem('chatSessionId', storedSessionId);
    }
    setSessionId(storedSessionId);
  }, []);

  // Close dropdown on outside click
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setIsSessionDropdownOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  useEffect(() => {
    if (editingWidget) {
      const isSimulation = editingWidget.widget_type === 'simulation';
      const editMessage: Message = {
        id: `edit_context_${Date.now()}`,
        content: isSimulation
          ? `📝 **Editing Simulator:** "${editingWidget.title}"\n\nThis is a ${editingWidget.simulation_config?.simulation_type?.replace('_', ' ')} simulator.\n\nWhat would you like to change? For example:\n• "Change the default loan amount to $500,000"\n• "Update the title"\n• "Set the default interest rate to 5%"`
          : `📝 **Editing Widget:** "${editingWidget.title}"\n\nI'm ready to help you modify this ${editingWidget.data_mode === 'dynamic' ? 'dynamic' : 'static'} ${editingWidget.config.chartType || 'chart'} widget.\n\nWhat would you like to change?`,
        role: 'assistant',
        timestamp: new Date(),
      };
      setMessages(prev => [...prev, editMessage]);
    }
  }, [editingWidget]);

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  const handleSendMessage = async () => {
    if (!inputValue.trim() || isLoading || !sessionId) return;

    const userMessage: Message = {
      id: `user_${Date.now()}`,
      content: inputValue,
      role: 'user',
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    const currentInput = inputValue;
    setInputValue('');
    setIsLoading(true);

    try {
      const isWidgetRequest = activeTab === 'ai-module' ||
        /create.*(chart|graph|visual|widget|pie|bar|line|simulator|calculator|projection|what.?if)/i.test(currentInput) ||
        /show.*as.*(chart|graph|pie|bar)/i.test(currentInput) ||
        /(loan|mortgage|savings|budget|retirement|emergency).*(calculator|simulator|planner|projector)/i.test(currentInput);

      const requestBody: any = {
        messages: [{ role: 'user', content: currentInput }],
        session_id: sessionId,
        user_id: userId,
        create_widget: isWidgetRequest,
      };

      if (editingWidget) {
        requestBody.edit_widget = {
          widget_id: editingWidget.id,
          title: editingWidget.title,
          description: editingWidget.description,
          widget_type: editingWidget.widget_type,
          chart_type: editingWidget.config.chartType,
          data_mode: editingWidget.data_mode,
          query_config: editingWidget.query_config,
          simulation_config: editingWidget.simulation_config,
        };
      }

      const response = await fetch(`${API_URL}/chatbot`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-User-Id': userId,
        },
        body: JSON.stringify(requestBody),
      });

      if (!response.ok) {
        throw new Error('Failed to get response');
      }

      const data = await response.json();

      const assistantMessage: Message = {
        id: `assistant_${Date.now()}`,
        content: data.response,
        role: 'assistant',
        timestamp: new Date(),
        widgetCreated: data.widget_created,
        widgetUpdated: data.widget_updated,
        widgetMode: data.widget_mode,
        widgetType: data.widget_type,
        simulationType: data.simulation_type,
      };

      setMessages((prev) => [...prev, assistantMessage]);

      if (data.widget_updated && onClearEditingWidget) {
        onClearEditingWidget();
      }

    } catch (error) {
      console.error('Chat error:', error);
      const errorMessage: Message = {
        id: `error_${Date.now()}`,
        content: 'Sorry, I encountered an error. Please try again.',
        role: 'assistant',
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSendMessage();
    }
  };

  const handleCancelEdit = () => {
    if (onClearEditingWidget) {
      onClearEditingWidget();
    }
  };

  // Toggle dropdown & lazy-load sessions
  const handleToggleDropdown = () => {
    const opening = !isSessionDropdownOpen;
    setIsSessionDropdownOpen(opening);
    if (opening) {
      loadSessions();
    }
  };

  // Get simulation type icon and color
  const getSimulationIcon = (simType: string | undefined) => {
    switch (simType) {
      case 'loan_repayment': return { icon: Home, color: 'text-blue-600', bg: 'bg-blue-50', border: 'border-blue-200' };
      case 'savings_projector': return { icon: PiggyBank, color: 'text-green-600', bg: 'bg-green-50', border: 'border-green-200' };
      case 'budget_planner': return { icon: Target, color: 'text-purple-600', bg: 'bg-purple-50', border: 'border-purple-200' };
      case 'retirement_calculator': return { icon: TrendingUp, color: 'text-indigo-600', bg: 'bg-indigo-50', border: 'border-indigo-200' };
      case 'emergency_fund': return { icon: Calculator, color: 'text-orange-600', bg: 'bg-orange-50', border: 'border-orange-200' };
      default: return { icon: Sliders, color: 'text-amber-600', bg: 'bg-amber-50', border: 'border-amber-200' };
    }
  };

  /** Format a timestamp string for display */
  const formatSessionDate = (dateStr: string) => {
    try {
      const d = new Date(dateStr);
      const now = new Date();
      const diffMs = now.getTime() - d.getTime();
      const diffMins = Math.floor(diffMs / 60000);
      if (diffMins < 1) return 'just now';
      if (diffMins < 60) return `${diffMins}m ago`;
      const diffHrs = Math.floor(diffMins / 60);
      if (diffHrs < 24) return `${diffHrs}h ago`;
      const diffDays = Math.floor(diffHrs / 24);
      if (diffDays < 7) return `${diffDays}d ago`;
      return d.toLocaleDateString();
    } catch {
      return '';
    }
  };

  // Quick suggestion buttons
  const quickSuggestions = editingWidget ? [
    { text: 'Change to a bar chart', icon: BarChart3, color: 'text-blue-600' },
    { text: 'Change to a pie chart', icon: PieChart, color: 'text-purple-600' },
    { text: 'Update the time range', icon: TrendingUp, color: 'text-green-600' },
  ] : activeTab === 'ai-module' ? [
    // Mix of chart and simulator suggestions
    { text: 'Create a spending by category chart', icon: PieChart, color: 'text-blue-600' },
    { text: 'Build a loan repayment calculator', icon: Home, color: 'text-amber-600' },
    { text: 'Create a savings projector', icon: PiggyBank, color: 'text-green-600' },
    { text: 'Make a retirement calculator', icon: TrendingUp, color: 'text-indigo-600' },
  ] : [
    { text: 'What are my account balances?', icon: BarChart3, color: 'text-blue-600' },
    { text: 'Show my recent transactions', icon: TrendingUp, color: 'text-green-600' },
    { text: 'Create a budget planner', icon: Target, color: 'text-purple-600' },
  ];

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="bg-gradient-to-r from-blue-600 to-blue-700 text-white px-4 py-3 rounded-t-xl">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Bot className="h-5 w-5" />
            <span className="font-semibold">Banking Assistant</span>
          </div>

          {/* Session picker */}
          <div className="relative" ref={dropdownRef}>
            <button
              onClick={handleToggleDropdown}
              className="flex items-center gap-1 text-xs bg-blue-500/40 hover:bg-blue-500/60 rounded-full px-2.5 py-1 transition-colors"
              title="Switch session"
            >
              <History className="h-3.5 w-3.5" />
              <span className="max-w-[100px] truncate">
                {sessions.find(s => (s.id || s.session_id) === sessionId)?.updated_at
                  ? formatSessionDate(sessions.find(s => (s.id || s.session_id) === sessionId)!.updated_at || sessions.find(s => (s.id || s.session_id) === sessionId)!.created_at)
                  : 'Sessions'}
              </span>
              <ChevronDown className={`h-3 w-3 transition-transform ${isSessionDropdownOpen ? 'rotate-180' : ''}`} />
            </button>

            {isSessionDropdownOpen && (
              <div className="absolute right-0 top-full mt-1 w-64 bg-white rounded-lg shadow-xl border border-gray-200 z-50 overflow-hidden">
                {/* New session button */}
                <button
                  onClick={startNewSession}
                  className="w-full flex items-center gap-2 px-3 py-2.5 text-sm text-blue-700 bg-blue-50 hover:bg-blue-100 transition-colors border-b border-gray-100"
                >
                  <Plus className="h-4 w-4" />
                  <span className="font-medium">New Session</span>
                </button>

                {/* Session list */}
                <div className="max-h-48 overflow-y-auto">
                  {isLoadingSessions ? (
                    <div className="flex items-center justify-center gap-2 py-4 text-gray-400 text-xs">
                      <Loader className="h-3.5 w-3.5 animate-spin" />
                      Loading sessions…
                    </div>
                  ) : sessions.length === 0 ? (
                    <div className="py-4 text-center text-gray-400 text-xs">
                      No previous sessions
                    </div>
                  ) : (
                    sessions.map((s) => {
                      const sid = s.id || s.session_id || '';
                      const isActive = sid === sessionId;
                      return (
                        <button
                          key={sid}
                          onClick={() => switchToSession(s)}
                          className={`w-full text-left px-3 py-2 text-sm transition-colors border-b border-gray-50 last:border-0 ${
                            isActive
                              ? 'bg-blue-50 text-blue-800'
                              : 'text-gray-700 hover:bg-gray-50'
                          }`}
                        >
                          <div className="flex items-center justify-between">
                            <span className="font-medium truncate max-w-[140px]">
                              {s.title || 'Untitled'}
                            </span>
                            <span className="text-[10px] text-gray-400 flex-shrink-0 ml-2">
                              {formatSessionDate(s.updated_at || s.created_at)}
                            </span>
                          </div>
                          <p className="text-[10px] text-gray-400 truncate mt-0.5">
                            {s.updated_at ? `Last active: ${formatSessionDate(s.updated_at)}` : s.created_at ? `Created: ${formatSessionDate(s.created_at)}` : ''}
                          </p>
                        </button>
                      );
                    })
                  )}
                </div>
              </div>
            )}
          </div>
        </div>

        {activeTab === 'ai-module' && !editingWidget && (
          <p className="text-xs text-blue-200 mt-1">
            Ask me to create charts or interactive simulators
          </p>
        )}
      </div>

      {/* Editing Widget Banner */}
      {editingWidget && (
        <div className={`border-b px-4 py-3 ${
          editingWidget.widget_type === 'simulation'
            ? 'bg-gradient-to-r from-amber-50 to-orange-50 border-amber-200'
            : 'bg-gradient-to-r from-indigo-50 to-purple-50 border-indigo-200'
        }`}>
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              {editingWidget.widget_type === 'simulation' ? (
                <Sliders className="h-4 w-4 text-amber-600" />
              ) : (
                <Edit3 className="h-4 w-4 text-indigo-600" />
              )}
              <div>
                <p className={`text-sm font-medium ${
                  editingWidget.widget_type === 'simulation' ? 'text-amber-900' : 'text-indigo-900'
                }`}>
                  Editing: {editingWidget.title}
                </p>
                <p className={`text-xs ${
                  editingWidget.widget_type === 'simulation' ? 'text-amber-600' : 'text-indigo-600'
                }`}>
                  {editingWidget.widget_type === 'simulation'
                    ? `🎛️ ${editingWidget.simulation_config?.simulation_type?.replace('_', ' ')}`
                    : `${editingWidget.data_mode === 'dynamic' ? '🔄 Dynamic' : '📊 Static'} • ${editingWidget.config.chartType} chart`
                  }
                </p>
              </div>
            </div>
            <button
              onClick={handleCancelEdit}
              className={`p-1 rounded transition-colors ${
                editingWidget.widget_type === 'simulation'
                  ? 'text-amber-400 hover:text-amber-600 hover:bg-amber-100'
                  : 'text-indigo-400 hover:text-indigo-600 hover:bg-indigo-100'
              }`}
              title="Cancel editing"
            >
              <X className="h-4 w-4" />
            </button>
          </div>
        </div>
      )}

      {/* Messages */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4 bg-gray-50">
        {messages.map((message) => (
          <div
            key={message.id}
            className={`flex ${message.role === 'user' ? 'justify-end' : 'justify-start'}`}
          >
            <div
              className={`max-w-[85%] rounded-2xl px-4 py-2.5 ${
                message.role === 'user'
                  ? 'bg-blue-600 text-white rounded-br-md'
                  : 'bg-white text-gray-800 shadow-sm border border-gray-100 rounded-bl-md'
              }`}
            >
              <div className="flex items-start gap-2">
                {message.role === 'assistant' && (
                  <Bot className="h-4 w-4 mt-0.5 text-blue-600 flex-shrink-0" />
                )}
                <div className="flex-1">
                  <p className="text-sm whitespace-pre-wrap">{message.content}</p>

                  {/* Widget creation indicator */}
                  {message.widgetCreated && message.widgetType === 'simulation' && (
                    <div className={`mt-2 p-2 rounded-lg text-xs ${getSimulationIcon(message.simulationType).bg} border ${getSimulationIcon(message.simulationType).border}`}>
                      <div className="flex items-center gap-1.5">
                        <Sliders className={`h-3.5 w-3.5 ${getSimulationIcon(message.simulationType).color}`} />
                        <span className={`font-medium ${getSimulationIcon(message.simulationType).color}`}>
                          🎛️ Interactive simulator created!
                        </span>
                      </div>
                      <p className="mt-1 text-gray-600">
                        Check the AI Module tab to use your new {message.simulationType?.replace('_', ' ')}!
                      </p>
                    </div>
                  )}

                  {message.widgetCreated && message.widgetType !== 'simulation' && (
                    <div className={`mt-2 p-2 rounded-lg text-xs ${
                      message.widgetMode === 'dynamic'
                        ? 'bg-gradient-to-r from-cyan-50 to-indigo-50 border border-cyan-200'
                        : 'bg-gradient-to-r from-indigo-50 to-purple-50 border border-indigo-200'
                    }`}>
                      <div className="flex items-center gap-1.5">
                        {message.widgetMode === 'dynamic' ? (
                          <>
                            <Zap className="h-3.5 w-3.5 text-cyan-600" />
                            <span className="font-medium text-cyan-700">
                              🔄 Dynamic widget created (refreshable)
                            </span>
                          </>
                        ) : (
                          <>
                            <Database className="h-3.5 w-3.5 text-purple-600" />
                            <span className="font-medium text-purple-700">
                              📊 Static widget created
                            </span>
                          </>
                        )}
                      </div>
                      <p className="mt-1 text-gray-600">
                        Check the AI Module tab to see your new visualization!
                      </p>
                    </div>
                  )}

                  {/* Widget updated indicator */}
                  {message.widgetUpdated && (
                    <div className="mt-2 p-2 rounded-lg text-xs bg-gradient-to-r from-green-50 to-emerald-50 border border-green-200">
                      <div className="flex items-center gap-1.5">
                        <Edit3 className="h-3.5 w-3.5 text-green-600" />
                        <span className="font-medium text-green-700">
                          ✓ Widget updated successfully
                        </span>
                      </div>
                      <p className="mt-1 text-gray-600">
                        Refresh the AI Module tab to see your changes!
                      </p>
                    </div>
                  )}
                </div>
                {message.role === 'user' && (
                  <User className="h-4 w-4 mt-0.5 flex-shrink-0" />
                )}
              </div>
            </div>
          </div>
        ))}

        {isLoading && (
          <div className="flex justify-start">
            <div className="bg-white rounded-2xl rounded-bl-md px-4 py-3 shadow-sm border border-gray-100">
              <div className="flex items-center gap-2">
                <Bot className="h-4 w-4 text-blue-600" />
                <Loader className="h-4 w-4 animate-spin text-blue-600" />
                <span className="text-sm text-gray-500">Thinking...</span>
              </div>
            </div>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* Quick Suggestions */}
      {messages.length <= 2 && (
        <div className="px-4 py-2 bg-gray-50 border-t border-gray-100">
          <p className="text-xs text-gray-500 mb-2">
            {editingWidget ? 'Quick edits:' : 'Quick suggestions:'}
          </p>
          <div className="flex flex-wrap gap-2">
            {quickSuggestions.map((suggestion, index) => (
              <button
                key={index}
                onClick={() => setInputValue(suggestion.text)}
                className="flex items-center gap-1.5 text-xs px-3 py-1.5 bg-white border border-gray-200 rounded-full hover:border-blue-300 hover:bg-blue-50 transition-colors"
              >
                <suggestion.icon className={`h-3 w-3 ${suggestion.color}`} />
                <span className="text-gray-700 truncate max-w-[180px]">{suggestion.text}</span>
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Input */}
      <div className="p-3 bg-white border-t border-gray-200 rounded-b-xl">
        <div className="flex items-center gap-2">
          <input
            type="text"
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            onKeyPress={handleKeyPress}
            placeholder={
              editingWidget
                ? `Describe changes to "${editingWidget.title}"...`
                : activeTab === 'ai-module'
                  ? "Ask me to create a chart or simulator..."
                  : "Ask me anything about your finances..."
            }
            className="flex-1 px-4 py-2 border border-gray-200 rounded-full text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            disabled={isLoading}
          />
          <button
            onClick={handleSendMessage}
            disabled={!inputValue.trim() || isLoading}
            className="p-2 bg-blue-600 text-white rounded-full hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            <Send className="h-4 w-4" />
          </button>
        </div>
      </div>
    </div>
  );
};

export default ChatBot;
