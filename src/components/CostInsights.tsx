import React, { useEffect, useMemo, useState } from 'react';
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from 'recharts';
import { DollarSign } from 'lucide-react';
import { AnalyticsAPI, type CostSummary } from '../services/analyticsApi';

const COLORS = ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#F97316'];

const CostInsights: React.FC = () => {
  const [summary, setSummary] = useState<CostSummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const load = async () => {
      try {
        setLoading(true);
        const data = await AnalyticsAPI.getCostSummary(7);
        setSummary(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load cost insights');
      } finally {
        setLoading(false);
      }
    };

    load();
  }, []);

  const pieData = useMemo(() => {
    if (!summary) return [];
    return summary.by_agent.slice(0, 6).map((item, index) => ({
      ...item,
      fill: COLORS[index % COLORS.length],
    }));
  }, [summary]);

  if (loading) {
    return (
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-center h-48">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Cost Insights (last 7 days)</h3>
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">{error}</div>
      </div>
    );
  }

  if (!summary || (summary.total_tokens === 0 && summary.total_cost_usd === 0)) {
    return (
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Cost Insights (last 7 days)</h3>
        <p className="text-gray-500">No tokenized agent activity in the selected period.</p>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 space-y-6">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold text-gray-900">Cost Insights (last 7 days)</h3>
        <DollarSign className="h-5 w-5 text-gray-400" />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-blue-50 rounded-lg p-4">
          <p className="text-sm text-blue-700">Total Cost</p>
          <p className="text-xl font-bold text-blue-900">${summary.total_cost_usd.toFixed(6)}</p>
        </div>
        <div className="bg-green-50 rounded-lg p-4">
          <p className="text-sm text-green-700">Total Tokens</p>
          <p className="text-xl font-bold text-green-900">{summary.total_tokens.toLocaleString()}</p>
        </div>
        <div className="bg-yellow-50 rounded-lg p-4">
          <p className="text-sm text-yellow-700">Conversations</p>
          <p className="text-xl font-bold text-yellow-900">{summary.conversations}</p>
        </div>
        <div className="bg-purple-50 rounded-lg p-4">
          <p className="text-sm text-purple-700">Avg Cost / Conversation</p>
          <p className="text-xl font-bold text-purple-900">${summary.avg_cost_per_conversation.toFixed(6)}</p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div>
          <h4 className="text-sm font-semibold text-gray-800 mb-3">Per-Agent Breakdown</h4>
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="text-left text-gray-500 border-b border-gray-200">
                  <th className="py-2 pr-2">Agent</th>
                  <th className="py-2 pr-2">Cost (USD)</th>
                  <th className="py-2">Share</th>
                </tr>
              </thead>
              <tbody>
                {summary.by_agent.map((agent) => (
                  <tr key={agent.agent_name} className="border-b border-gray-100">
                    <td className="py-2 pr-2 text-gray-900">{agent.agent_name}</td>
                    <td className="py-2 pr-2 text-gray-700">${agent.cost_usd.toFixed(6)}</td>
                    <td className="py-2 text-gray-700">{agent.percent.toFixed(2)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div>
          <h4 className="text-sm font-semibold text-gray-800 mb-3">Cost Share by Agent</h4>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={pieData}
                  dataKey="cost_usd"
                  nameKey="agent_name"
                  cx="50%"
                  cy="50%"
                  outerRadius={90}
                  innerRadius={48}
                >
                  {pieData.map((entry, index) => (
                    <Cell key={`cell-${entry.agent_name}-${index}`} fill={entry.fill} />
                  ))}
                </Pie>
                <Tooltip formatter={(value: number) => [`$${value.toFixed(6)}`, 'Cost']} />
              </PieChart>
            </ResponsiveContainer>
          </div>
          <div className="grid grid-cols-2 gap-2 mt-2">
            {pieData.map((item) => (
              <div key={item.agent_name} className="flex items-center gap-2 text-sm text-gray-600">
                <span className="w-3 h-3 rounded-full" style={{ backgroundColor: item.fill }}></span>
                <span className="truncate">{item.agent_name}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};

export default CostInsights;
