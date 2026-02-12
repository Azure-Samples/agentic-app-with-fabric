import React, { useState, useEffect } from 'react';
import { AnalyticsAPI } from '../services/analyticsApi';
import type { ChatSession } from '../types/analytics';
import { Calendar, MessageSquare, Download, Trash2, RefreshCw, AlertCircle } from 'lucide-react';

const ChatSessions: React.FC = () => {
  const [sessions, setSessions] = useState<ChatSession[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isConnected, setIsConnected] = useState(false);

  useEffect(() => {
    loadSessions();
  }, []);

  const loadSessions = async () => {
    try {
      setLoading(true);
      setError(null);
      const sessionsData = await AnalyticsAPI.getChatSessions();
      setSessions(sessionsData);
      setIsConnected(true);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to load sessions';
      setError(errorMessage);
      setIsConnected(false);
      console.error('Failed to load sessions:', err);
    } finally {
      setLoading(false);
    }
  };

  // const handleExportSession = async (sessionId: string) => {
  //   try {
  //     const blob = await AnalyticsAPI.exportChatSession(sessionId);
  //     const url = window.URL.createObjectURL(blob);
  //     const a = document.createElement('a');
  //     a.style.display = 'none';
  //     a.href = url;
  //     a.download = `chat-session-${sessionId}.json`;
  //     document.body.appendChild(a);
  //     a.click();
  //     window.URL.revokeObjectURL(url);
  //     document.body.removeChild(a);
  //   } catch (err) {
  //     setError(err instanceof Error ? err.message : 'Failed to export session');
  //   }
  // };

  const handleDeleteSession = async (sessionId: string) => {
    if (!confirm('Are you sure you want to delete this session?')) return;
    
    try {
      await AnalyticsAPI.clearSession(sessionId);
      setSessions(sessions.filter(s => (s.id || s.session_id) !== sessionId));
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to delete session');
    }
  };

  const handleClearAllHistory = async () => {
    if (!confirm('Are you sure you want to clear ALL chat history? This cannot be undone.')) return;
    
    try {
      await AnalyticsAPI.clearChatHistory();
      setSessions([]);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to clear chat history');
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
        <span className="ml-2 text-gray-600">Loading chat sessions...</span>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div className="flex items-center gap-3">
          <h2 className="text-2xl font-bold text-gray-900">Chat Sessions</h2>
          {/* Connection Status Indicator */}
          <div className={`flex items-center gap-1 px-2 py-1 rounded text-xs ${
            isConnected ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
          }`}>
            <div className={`w-2 h-2 rounded-full ${
              isConnected ? 'bg-green-500' : 'bg-red-500'
            }`}></div>
            {isConnected ? 'Analytics Connected' : 'Analytics Disconnected'}
          </div>
        </div>
        <div className="flex gap-2">
          <button
            onClick={loadSessions}
            className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 flex items-center gap-2"
          >
            <RefreshCw className="h-4 w-4" />
            Refresh
          </button>
          <button
            onClick={handleClearAllHistory}
            className="bg-red-600 text-white px-4 py-2 rounded-lg hover:bg-red-700 flex items-center gap-2"
            disabled={!isConnected}
          >
            <Trash2 className="h-4 w-4" />
            Clear All History
          </button>
        </div>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded flex items-center gap-2">
          <AlertCircle className="h-5 w-5" />
          <div>
            <strong>Error:</strong> {error}
            {error.includes('Failed to fetch') && (
              <div className="mt-1 text-sm">
                Make sure the analytics server is running on port 5002. 
                Run: <code className="bg-red-100 px-1 rounded">python run_both_apps.py</code>
              </div>
            )}
          </div>
        </div>
      )}

      <div className="grid gap-4">
        {sessions.length === 0 ? (
          <div className="text-center py-12 text-gray-500">
            <MessageSquare className="h-12 w-12 mx-auto mb-4 opacity-50" />
            <p>No chat sessions found</p>
            {!isConnected && (
              <p className="text-sm mt-2">Start the analytics server to view chat sessions</p>
            )}
          </div>
        ) : (
          sessions.map((session) => (
            <div
              key={(session.id || session.session_id || '')}
              className="bg-white border border-gray-200 rounded-lg p-6 hover:border-blue-300 transition-colors"
            >
              <div className="flex justify-between items-start">
                <div className="flex-1">
                  <h3 className="text-lg font-semibold text-gray-900">
                    {session.title || `Session ${(session.id || session.session_id || '').slice(0, 8)}`}
                  </h3>
                  <p className="text-sm text-gray-500 mt-1">
                    Session ID: {(session.id || session.session_id || '')}
                  </p>
                  <div className="flex items-center gap-4 mt-2 text-sm text-gray-600">
                    <div className="flex items-center gap-1">
                      <Calendar className="h-4 w-4" />
                      {new Date(session.created_at).toLocaleString()}
                    </div>
                    <div className="flex items-center gap-1">
                      <MessageSquare className="h-4 w-4" />
                      User: {session.user_id}
                    </div>
                  </div>
                </div>
                <div className="flex gap-2">
                  {/* <button
                    onClick={() => handleExportSession((session.id || session.session_id || ''))}
                    className="bg-blue-600 text-white px-3 py-1 rounded hover:bg-blue-700 flex items-center gap-1"
                    disabled={!isConnected}
                  >
                    <Download className="h-4 w-4" />
                    Export
                  </button> */}
                  <button
                    onClick={() => handleDeleteSession((session.id || session.session_id || ''))}
                    className="bg-red-600 text-white px-3 py-1 rounded hover:bg-red-700 flex items-center gap-1"
                    disabled={!isConnected}
                  >
                    <Trash2 className="h-4 w-4" />
                    Delete
                  </button>
                </div>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  );
};

export default ChatSessions;