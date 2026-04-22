import React, { useState, useEffect } from 'react';
import { AnalyticsAPI } from '../services/analyticsApi';
import type { ChatSession } from '../types/analytics';
import { Calendar, MessageSquare, Download, Trash2, Plus } from 'lucide-react';
import CostInsights from './CostInsights';

const ChatSessions: React.FC = () => {
  const [sessions, setSessions] = useState<ChatSession[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedSession, setSelectedSession] = useState<string | null>(null);

  useEffect(() => {
    loadSessions();
  }, []);

  const loadSessions = async () => {
    try {
      setLoading(true);
      const sessionsData = await AnalyticsAPI.getChatSessions();
      setSessions(sessionsData);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load sessions');
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
  //   } catch (err) {
  //     setError(err instanceof Error ? err.message : 'Failed to export session');
  //   }
  // };

  const handleDeleteSession = async (sessionId: string) => {
    if (!confirm('Are you sure you want to delete this session?')) return;
    
    try {
      await AnalyticsAPI.clearSession(sessionId);
      setSessions(sessions.filter(s => s.session_id !== sessionId));
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
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <CostInsights />

      <div className="flex justify-between items-center">
        <h2 className="text-2xl font-bold text-gray-900">Chat Sessions</h2>
        <button
          onClick={handleClearAllHistory}
          className="bg-red-600 text-white px-4 py-2 rounded-lg hover:bg-red-700 flex items-center gap-2"
        >
          <Trash2 className="h-4 w-4" />
          Clear All History
        </button>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
          {error}
        </div>
      )}

      <div className="grid gap-4">
        {sessions.length === 0 ? (
          <div className="text-center py-12 text-gray-500">
            <MessageSquare className="h-12 w-12 mx-auto mb-4 opacity-50" />
            <p>No chat sessions found</p>
          </div>
        ) : (
          sessions.map((session) => (
            <div
              key={session.session_id}
              className="bg-white border border-gray-200 rounded-lg p-6 hover:border-blue-300 transition-colors"
            >
              <div className="flex justify-between items-start">
                <div className="flex-1">
                  <h3 className="text-lg font-semibold text-gray-900">
                    {session.title || `Session ${session.session_id.slice(0, 8)}`}
                  </h3>
                  <p className="text-sm text-gray-500 mt-1">
                    Session ID: {session.session_id}
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
                    onClick={() => handleExportSession(session.session_id)}
                    className="bg-blue-600 text-white px-3 py-1 rounded hover:bg-blue-700 flex items-center gap-1"
                  >
                    <Download className="h-4 w-4" />
                    Export
                  </button> */}
                  <button
                    onClick={() => handleDeleteSession(session.session_id)}
                    className="bg-red-600 text-white px-3 py-1 rounded hover:bg-red-700 flex items-center gap-1"
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
