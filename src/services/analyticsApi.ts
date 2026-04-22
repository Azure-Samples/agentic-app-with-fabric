import type {
  ChatSession,
  ConversationMessage,
  // ChatHistory,
  // ToolUsage,
  ToolDefinition,
  // AnalyticsData
} from '../types/analytics';

// Analytics API runs on port 5002
import { ANALYTICS_API_URL } from '../apiConfig';

export interface CostSummary {
  total_prompt_tokens: number
  total_completion_tokens: number
  total_tokens: number
  total_cost_usd: number
  conversations: number
  avg_cost_per_conversation: number
  by_agent: Array<{ agent_name: string; cost_usd: number; percent: number }>
  by_model: Array<{ model_name: string; cost_usd: number }>
}

export interface TraceCost {
  trace_id: string
  total_cost_usd: number
  per_step: Array<{
    agent_name: string
    prompt_tokens: number
    completion_tokens: number
    total_tokens: number
    cost_usd: number
    model_name: string | null
  }>
}

export class AnalyticsAPI {
  // Chat Sessions
  static async getChatSessions(): Promise<ChatSession[]> {
    try {
      const response = await fetch(`${ANALYTICS_API_URL}/chat/sessions`);
      if (!response.ok) {
        throw new Error(`Failed to fetch chat sessions: ${response.status} ${response.statusText}`);
      }
      return response.json();
    } catch (error) {
      console.error('Error fetching chat sessions:', error);
      throw error;
    }
  }

  /**
   * Fetch sessions for a specific user from Cosmos DB longterm_memory.
   */
  static async getChatSessionsByUser(userId: string): Promise<ChatSession[]> {
    try {
      const response = await fetch(
        `${ANALYTICS_API_URL}/chat/sessions?user_id=${encodeURIComponent(userId)}`
      );
      if (!response.ok) {
        throw new Error(`Failed to fetch user sessions: ${response.status} ${response.statusText}`);
      }
      return response.json();
    } catch (error) {
      console.error('Error fetching user sessions:', error);
      throw error;
    }
  }

  /**
   * Fetch conversation history for a session from Cosmos DB longterm_memory.
   */
  static async getChatHistory(sessionId: string, userId: string): Promise<ConversationMessage[]> {
    try {
      const response = await fetch(
        `${ANALYTICS_API_URL}/chat/history/${encodeURIComponent(sessionId)}?user_id=${encodeURIComponent(userId)}`
      );
      if (!response.ok) {
        throw new Error(`Failed to fetch chat history: ${response.status} ${response.statusText}`);
      }
      // The endpoint returns [{trace_id, message_type, content, trace_end}, ...]
      // Map to ConversationMessage shape
      const raw = await response.json();
      return raw.map((m: any) => ({
        type: m.message_type === 'human' ? 'human' : 'ai',
        content: m.content || '',
        timestamp: m.trace_end || '',
      }));
    } catch (error) {
      console.error('Error fetching chat history:', error);
      throw error;
    }
  }

  static async createChatSession(sessionData: Partial<ChatSession>): Promise<ChatSession> {
    try {
      const response = await fetch(`${ANALYTICS_API_URL}/chat/sessions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(sessionData),
      });
      if (!response.ok) {
        throw new Error(`Failed to create chat session: ${response.status} ${response.statusText}`);
      }
      return response.json();
    } catch (error) {
      console.error('Error creating chat session:', error);
      throw error;
    }
  }
  // Tool Definitions
  static async getToolDefinitions(): Promise<ToolDefinition[]> {
    try {
      const response = await fetch(`${ANALYTICS_API_URL}/tools/definitions`);
      if (!response.ok) {
        throw new Error(`Failed to fetch tool definitions: ${response.status} ${response.statusText}`);
      }
      return response.json();
    } catch (error) {
      console.error('Error fetching tool definitions:', error);
      throw error;
    }
  }

  static async createToolDefinition(toolData: Omit<ToolDefinition, 'id' | 'created_at' | 'updated_at'>): Promise<ToolDefinition> {
    try {
      const response = await fetch(`${ANALYTICS_API_URL}/tools/definitions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(toolData),
      });
      if (!response.ok) {
        throw new Error(`Failed to create tool definition: ${response.status} ${response.statusText}`);
      }
      return response.json();
    } catch (error) {
      console.error('Error creating tool definition:', error);
      throw error;
    }
  }

  // Admin Functions
  static async clearChatHistory(): Promise<void> {
    try {
      const response = await fetch(`${ANALYTICS_API_URL}/admin/clear-chat-history`, {
        method: 'DELETE',
      });
      if (!response.ok) {
        throw new Error(`Failed to clear chat history: ${response.status} ${response.statusText}`);
      }
    } catch (error) {
      console.error('Error clearing chat history:', error);
      throw error;
    }
  }

  static async clearSession(sessionId: string): Promise<void> {
    try {
      const response = await fetch(`${ANALYTICS_API_URL}/admin/clear-session/${sessionId}`, {
        method: 'DELETE',
      });
      if (!response.ok) {
        throw new Error(`Failed to clear session: ${response.status} ${response.statusText}`);
      }
    } catch (error) {
      console.error('Error clearing session:', error);
      throw error;
    }
  }

  static async getCostSummary(days: number = 7): Promise<CostSummary> {
    try {
      const response = await fetch(`${ANALYTICS_API_URL}/analytics/cost-summary?days=${days}`);
      if (!response.ok) {
        throw new Error(`Failed to fetch cost summary: ${response.status} ${response.statusText}`);
      }
      return response.json();
    } catch (error) {
      console.error('Error fetching cost summary:', error);
      throw error;
    }
  }

  static async getTraceCost(traceId: string): Promise<TraceCost> {
    try {
      const response = await fetch(`${ANALYTICS_API_URL}/analytics/trace/${encodeURIComponent(traceId)}/cost`);
      if (!response.ok) {
        throw new Error(`Failed to fetch trace cost: ${response.status} ${response.statusText}`);
      }
      return response.json();
    } catch (error) {
      console.error('Error fetching trace cost:', error);
      throw error;
    }
  }
}
