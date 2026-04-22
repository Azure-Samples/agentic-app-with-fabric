CREATE TABLE [dbo].[chat_history] (
    [message_id]             VARCHAR (255)  NOT NULL,
    [session_id]             VARCHAR (255)  NOT NULL,
    [trace_id]               VARCHAR (255)  NOT NULL,
    [user_id]                VARCHAR (255)  NOT NULL,
    [agent_id]               VARCHAR (255)  NULL,
    [agent_name]             VARCHAR (255)  NULL,
    [routing_step]           INT            NULL,
    [message_type]           VARCHAR (50)   NOT NULL,
    [content]                NVARCHAR (MAX) NULL,
    [model_name]             VARCHAR (255)  NULL,
    [content_filter_results] NVARCHAR (MAX) NULL,
    [total_tokens]           INT            NULL,
    [completion_tokens]      INT            NULL,
    [prompt_tokens]          INT            NULL,
    [tool_id]                VARCHAR (255)  NULL,
    [tool_name]              VARCHAR (255)  NULL,
    [tool_input]             NVARCHAR (MAX) NULL,
    [tool_output]            NVARCHAR (MAX) NULL,
    [tool_call_id]           VARCHAR (255)  NULL,
    [finish_reason]          VARCHAR (255)  NULL,
    [response_time_ms]       INT            NULL,
    [trace_end]              DATETIME2 (7)  DEFAULT (getdate()) NULL,
    [estimated_cost_usd]     DECIMAL (10, 6) NULL,
    PRIMARY KEY CLUSTERED ([message_id] ASC)
);


GO

