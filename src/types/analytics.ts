export interface ChatSession {
  id: string;           // Cosmos DB document id (= session_id)
  session_id?: string;  // Alias kept for backwards compat
  user_id: string;
  title: string;
  created_at: string;
  updated_at: string;
}

export interface ConversationMessage {
  type: 'human' | 'ai';
  content: string;
  timestamp: string;
}

export interface ChatHistory {
  message_id: string;
  session_id: string;
  trace_id: string;
  user_id: string;
  agent_id?: string;
  message_type: 'human' | 'ai' | 'system' | 'tool_call' | 'tool_result';
  content: string;

  model_name: string;
  content_filter_results: Record<string, any>;
  total_tokens: number;
  completion_tokens: number;
  prompt_tokens: number;


  tool_call_id?: string;
  tool_input_id?: string;
  tool_name?: string;
  tool_input?: Record<string, any>;
  tool_output?: Record<string, any>;

  finish_reason?: string;
  response_time_ms?: number;
  trace_end?: Date;

}

export interface ToolUsage {
  tool_call_id: string;
  session_id: string;
  user_id: string;
  message_id: string;
  trace_id: string;
  tool_name: string;
  tool_input: Record<string, any>;
  tool_output?: Record<string, any>;
  tool_id?: string;
  status: 'pending' | 'success' | 'error' | 'timeout';
  tokens_used?: number;
}

export interface ToolDefinition {
  id: string;
  name: string;
  description?: string;
  input_schema: Record<string, any>;
  version: string;
  cost_per_call_cents: number;
  is_active: boolean;
  created_at: string;
  updated_at: string;
}

// export interface AnalyticsData {
//   totalSessions: number;
//   totalMessages: number;
//   totalToolCalls: number;
//   averageSessionLength: number;
//   mostUsedTools: Array<{
//     tool_name: string;
//     usage_count: number;
//     avg_execution_time: number;
//   }>;
//   errorRate: number;
//   totalCost: number;
// }