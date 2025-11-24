import os
import json
import logging
import asyncio
from typing import List, Dict, Any

from dotenv import load_dotenv
from openai import AzureOpenAI
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


# Load environment variables
load_dotenv()

MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://127.0.0.1:8000").rstrip("/")

# Azure OpenAI configuration
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o")
AZURE_API_VERSION = os.getenv("AZURE_API_VERSION", "2024-12-01-preview")

if not AZURE_OPENAI_KEY:
    raise RuntimeError("❌ AZURE_OPENAI_KEY not set in environment")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("FlightOps.MCPClient")

# Initialize Azure OpenAI client
client_azure = AzureOpenAI(
    api_key=AZURE_OPENAI_KEY,
    api_version=AZURE_API_VERSION,
    azure_endpoint=AZURE_OPENAI_ENDPOINT
)


class FlightOpsMCPClient:
    def __init__(self, base_url: str = None):
        self.base_url = (base_url or MCP_SERVER_URL).rstrip("/")
        self.session: ClientSession = None
        self._client_context = None

    
    async def connect(self):
        try:
            logger.info(f"Connecting to MCP server at {self.base_url}")
            self._client_context = streamablehttp_client(self.base_url)
            read_stream, write_stream, _ = await self._client_context.__aenter__()
            self.session = ClientSession(read_stream, write_stream)
            await self.session.__aenter__()
            await self.session.initialize()
            logger.info("✅ Connected to MCP server successfully")
        except Exception as e:
            logger.error(f"Failed to connect to MCP server: {e}")
            raise

    async def disconnect(self):
        try:
            if self.session:
                await self.session.__aexit__(None, None, None)
            if self._client_context:
                await self._client_context.__aexit__(None, None, None)
            logger.info("Disconnected from MCP server")
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")
temperature = 0.2
async def run_chat():
    async with AsyncClient(transport=HTTPTransport(MCP_SERVER_URL)) as session:
        tool_list = await session.get_tools()
        #build OpenAI tool definitions
        tools_for_openai = []
        for t in tools_list.tools:
            tools_for_openai.append({
                "name": t.name,
                "description": t.description or "",
                "parameters": t.inputSchema or {"type": "object", "properties": {}}
            })
        while True:
            user_query = input("you> ").strip()
            system_prompt=(
                "You are a friendly travel assistant . Use tools when needed to look up flight data."
                "If you call a tool, then above your answer include the tool call JSON as allowed by MCP."
            )
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_query}
            ]
            response = client_azure.chat.completions.create(
                model=AZURE_OPENAI_DEPLOYMENT,
                messages=messages,
                temperature=temperature,
                functons=tools_for_openai,
                function_call="auto"
            )
            msg=response.choices[0].message
            if msg.get("function_call"):
                func_name=msg["function_call"]["name"]
                args=json.loads(msg["function_call"]["arguments"]or "{}")
                #call through MCP
                tool_resp = await session.call_tool(name=func_name, arguments=args)
                result_content = tool_resp.content
                messages.append({"role": "assistant", "content": None, "tool_call": {"name": func_name, "arguments": args}})
                messages.append({"role": "tool", "name": func_name, "content":json.dumps(result_content)})
                
                #second pass to summarize
                second = client_azure.chat.completions.create(
                    model=AZURE_OPENAI_DEPLOYMENT,
                    messages=messages,
                    temperature=temperature,
                )
                final=second.choices[0].message.content
                print(f"assistant> {final}")
            else:
                print(f"assistant> {msg['content']or ""}")
if __name__ == "__main__":
    asyncio.run(run_chat())