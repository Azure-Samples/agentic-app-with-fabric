import React, { useState, useEffect, useRef } from 'react';
import { Send, Bot, User, Loader, Zap, Database, BarChart3, PieChart, TrendingUp, X, Edit3, Sliders, Calculator, Target, PiggyBank, Home, Wallet , CreditCard} from 'lucide-react';
import { API_URL } from '../apiConfig';
import type { AIWidget } from '../types/aiModule';

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
  widgetType?: 'chart' | 'simulation' | 'goal';
  goalType?: string;
  simulationType?: string;
}

const ChatBot: React.FC<ChatBotProps> = ({ userId, activeTab, editingWidget, onClearEditingWidget }) => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let storedSessionId = localStorage.getItem('chatSessionId');
    if (!storedSessionId) {
      storedSessionId = `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      localStorage.setItem('chatSessionId', storedSessionId);
    }
    setSessionId(storedSessionId);

    setMessages([
      {
        id: 'welcome',
        content: `Hi! I'm your banking assistant. I can help you with:\n\n• Check account balances and transactions\n• Transfer money between accounts\n• Answer questions about your finances\n• Create data charts 📊 (dynamic or static)\n• Build interactive simulators 🎛️ (What-If calculators)\n\nHow can I help you today?`,
        role: 'assistant',
        timestamp: new Date(),
      },
    ]);
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

  const getGoalIcon = (goalType: string | undefined) => {
  switch (goalType) {
    case 'savings': 
      return { icon: PiggyBank, color: 'text-green-600', bg: 'bg-green-50', border: 'border-green-200', label: 'Savings Goal' };
    case 'debt_payoff': 
      return { icon: CreditCard, color: 'text-red-600', bg: 'bg-red-50', border: 'border-red-200', label: 'Debt Payoff Goal' };
    case 'spending_limit': 
      return { icon: Wallet, color: 'text-amber-600', bg: 'bg-amber-50', border: 'border-amber-200', label: 'Spending Budget' };
    default: 
      return { icon: Target, color: 'text-green-600', bg: 'bg-green-50', border: 'border-green-200', label: 'Goal Tracker' };
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
    { text: 'Save $5000 for a vacation', icon: Target, color: 'text-green-600' },
    { text: 'Build a loan repayment calculator', icon: Home, color: 'text-amber-600' },
    { text: 'Track my restaurant spending budget', icon: Wallet, color: 'text-red-600' },
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

                  {/* Goal creation indicator */}
                  {message.widgetCreated && message.widgetType === 'goal' && (
                    <div className={`mt-2 p-2 rounded-lg text-xs ${getGoalIcon(message.goalType).bg} border ${getGoalIcon(message.goalType).border}`}>
                      <div className="flex items-center gap-1.5">
                        <Target className={`h-3.5 w-3.5 ${getGoalIcon(message.goalType).color}`} />
                        <span className={`font-medium ${getGoalIcon(message.goalType).color}`}>
                          🎯 {getGoalIcon(message.goalType).label} created!
                        </span>
                      </div>
                      <p className="mt-1 text-gray-600">
                        Check the AI Module tab to track your progress. It updates automatically!
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