import logging
import os
import streamlit as st
from model_serving_utils import (
    endpoint_supports_feedback, 
    query_endpoint, 
    query_endpoint_stream, 
    _get_endpoint_task_type,
)
from collections import OrderedDict
from messages import UserMessage, AssistantResponse, render_message

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
#adding the background background-image: url("https://cdn.wallpapersafari.com/88/75/cLUQqJ.jpg");
  
def wide_space_default():
    st.set_page_config(layout="wide")

wide_space_default()
page_element="""
<style>
[data-testid="stAppViewContainer"]{
  background-size: 1500px;
  background-position: center;
  background-repeat: no-repeat;
  background-filter: blur(10px);
}

</style>

"""

st.markdown(page_element, unsafe_allow_html=True)

import streamlit as st
import base64
image_path = "/Users/siddesh.ujjni/Desktop/databricks_apps_templates/app-templates/e2e-chatbot-app/imageslogo/generated-image.png"
def set_bg_local(image_path):
    with open(image_path, "rb") as img_file:
        encoded = base64.b64encode(img_file.read()).decode()
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-image: url("data:image/png;base64,{encoded}");
            background-size: 80%;
            background-filter: blur(20px);
            background-repeat: no-repeat;
            background-position: right;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

#set_bg_local(image_path)

#SERVING_ENDPOINT = 'https://e2-demo-field-eng.cloud.databricks.com/serving-endpoints/mas-bc0deddc-endpoint/invocations'
SERVING_ENDPOINT = os.getenv('SERVING_ENDPOINT')
assert SERVING_ENDPOINT, \
    ("Unable to determine serving endpoint to use for chatbot app. If developing locally, "
     "set the SERVING_ENDPOINT environment variable to the name of your serving endpoint. If "
     "deploying to a Databricks app, include a serving endpoint resource named "
     "'serving_endpoint' with CAN_QUERY permissions, as described in "
     "https://docs.databricks.com/aws/en/generative-ai/agent-framework/chat-app#deploy-the-databricks-app")

ENDPOINT_SUPPORTS_FEEDBACK = endpoint_supports_feedback(SERVING_ENDPOINT)

def reduce_chat_agent_chunks(chunks):
    """
    Reduce a list of ChatAgentChunk objects corresponding to a particular
    message into a single ChatAgentMessage
    """
    deltas = [chunk.delta for chunk in chunks]
    first_delta = deltas[0]
    result_msg = first_delta
    msg_contents = []
    
    # Accumulate tool calls properly
    tool_call_map = {}  # Map call_id to tool call for accumulation
    
    for delta in deltas:
        # Handle content
        if delta.content:
            msg_contents.append(delta.content)
            
        # Handle tool calls
        if hasattr(delta, 'tool_calls') and delta.tool_calls:
            for tool_call in delta.tool_calls:
                call_id = getattr(tool_call, 'id', None)
                tool_type = getattr(tool_call, 'type', "function")
                function_info = getattr(tool_call, 'function', None)
                if function_info:
                    func_name = getattr(function_info, 'name', "")
                    func_args = getattr(function_info, 'arguments', "")
                else:
                    func_name = ""
                    func_args = ""
                
                if call_id:
                    if call_id not in tool_call_map:
                        # New tool call
                        tool_call_map[call_id] = {
                            "id": call_id,
                            "type": tool_type,
                            "function": {
                                "name": func_name,
                                "arguments": func_args
                            }
                        }
                    else:
                        # Accumulate arguments for existing tool call
                        existing_args = tool_call_map[call_id]["function"]["arguments"]
                        tool_call_map[call_id]["function"]["arguments"] = existing_args + func_args

                        # Update function name if provided
                        if func_name:
                            tool_call_map[call_id]["function"]["name"] = func_name

        # Handle tool call IDs (for tool response messages)
        if hasattr(delta, 'tool_call_id') and delta.tool_call_id:
            result_msg = result_msg.model_copy(update={"tool_call_id": delta.tool_call_id})
    
    # Convert tool call map back to list
    if tool_call_map:
        accumulated_tool_calls = list(tool_call_map.values())
        result_msg = result_msg.model_copy(update={"tool_calls": accumulated_tool_calls})
    
    result_msg = result_msg.model_copy(update={"content": "".join(msg_contents)})
    return result_msg



# --- Init state ---
if "history" not in st.session_state:
    st.session_state.history = []

#st.title("🧱 Agentic Supervisor for Ag and Manufacturing")
mcCainLogo = "/Users/siddesh.ujjni/Desktop/databricks_apps_templates/app-templates/e2e-chatbot-app/imageslogo/McCain-Logo.svg"

st.logo(
    image=mcCainLogo,
    icon_image=None,  # Optional: A smaller icon for the main body if desired
    link=None, # Optional: Link the logo to a URL
    size="large",  # Optional: "small", "medium" (default), or "large"
)
st.markdown("""
    <style>
    img[data-testid="stLogo"] {
        height: 200px !important;
        width: auto !important;
    }
    </style>
    """, unsafe_allow_html=True)

import streamlit.components.v1 as components

# Sidebar navigation
page = st.sidebar.radio("McCain Agent:", ["Agentic Supervisor for Ag and Manufacturing", "Manufacturing Quality OEE Cost Analysis 🧱 "])
if page == "Agentic Supervisor for Ag and Manufacturing":
    st.title("🧠Enterprise AI Orchestrator for Agriculture & Manufacturing")
    #st.write(f"A multi agent supervisor that not only looks at structured data but also unstructured data along with Dashboards for visualizations all governed by Unity Catalog")
    #st.write(f"Endpoint name: `{SERVING_ENDPOINT}`")
    st.markdown("""
    <style>
    .stVerticalBlock {
    gap: 1rem !important; /* Adjust the gap as needed (0rem removes all space) */
    }
    .block-container {
        padding-top: 3rem;
        padding-bottom: 1rem;
    }
    </style>
""", unsafe_allow_html=True)
    # --- Render chat history ---
    for i, element in enumerate(st.session_state.history):
        element.render(i)

    def query_endpoint_and_render(task_type, input_messages):
        """Handle streaming response based on task type."""
        if task_type == "agent/v1/responses":
            return query_responses_endpoint_and_render(input_messages)
        elif task_type == "agent/v2/chat":
            return query_chat_agent_endpoint_and_render(input_messages)
        else:  # chat/completions
            return query_chat_completions_endpoint_and_render(input_messages)


    def query_chat_completions_endpoint_and_render(input_messages):
        """Handle ChatCompletions streaming format."""
        with st.chat_message("assistant"):
            response_area = st.empty()
            response_area.markdown("_Thinking..._")
            
            accumulated_content = ""
            request_id = None
            
            try:
                for chunk in query_endpoint_stream(
                    endpoint_name=SERVING_ENDPOINT,
                    messages=input_messages,
                    return_traces=ENDPOINT_SUPPORTS_FEEDBACK
                ):
                    if "choices" in chunk and chunk["choices"]:
                        delta = chunk["choices"][0].get("delta", {})
                        content = delta.get("content", "")
                        if content:
                            accumulated_content += content
                            response_area.markdown(accumulated_content)
                    
                    if "databricks_output" in chunk:
                        req_id = chunk["databricks_output"].get("databricks_request_id")
                        if req_id:
                            request_id = req_id
                
                return AssistantResponse(
                    messages=[{"role": "assistant", "content": accumulated_content}],
                    request_id=request_id
                )
            except Exception:
                response_area.markdown("_Ran into an error. Retrying without streaming..._")
                messages, request_id = query_endpoint(
                    endpoint_name=SERVING_ENDPOINT,
                    messages=input_messages,
                    return_traces=ENDPOINT_SUPPORTS_FEEDBACK
                )
                response_area.empty()
                with response_area.container():
                    for message in messages:
                        render_message(message)
                return AssistantResponse(messages=messages, request_id=request_id)


    def query_chat_agent_endpoint_and_render(input_messages):
        """Handle ChatAgent streaming format."""
        from mlflow.types.agent import ChatAgentChunk
        
        with st.chat_message("assistant"):
            response_area = st.empty()
            response_area.markdown("_Thinking..._")
            
            message_buffers = OrderedDict()
            request_id = None
            
            try:
                for raw_chunk in query_endpoint_stream(
                    endpoint_name=SERVING_ENDPOINT,
                    messages=input_messages,
                    return_traces=ENDPOINT_SUPPORTS_FEEDBACK
                ):
                    response_area.empty()
                    chunk = ChatAgentChunk.model_validate(raw_chunk)
                    delta = chunk.delta
                    message_id = delta.id

                    req_id = raw_chunk.get("databricks_output", {}).get("databricks_request_id")
                    if req_id:
                        request_id = req_id
                    if message_id not in message_buffers:
                        message_buffers[message_id] = {
                            "chunks": [],
                            "render_area": st.empty(),
                        }
                    message_buffers[message_id]["chunks"].append(chunk)
                    
                    partial_message = reduce_chat_agent_chunks(message_buffers[message_id]["chunks"])
                    render_area = message_buffers[message_id]["render_area"]
                    message_content = partial_message.model_dump_compat(exclude_none=True)
                    with render_area.container():
                        render_message(message_content)
                
                messages = []
                for msg_id, msg_info in message_buffers.items():
                    messages.append(reduce_chat_agent_chunks(msg_info["chunks"]))
                
                return AssistantResponse(
                    messages=[message.model_dump_compat(exclude_none=True) for message in messages],
                    request_id=request_id
                )
            except Exception:
                response_area.markdown("_Ran into an error. Retrying without streaming..._")
                messages, request_id = query_endpoint(
                    endpoint_name=SERVING_ENDPOINT,
                    messages=input_messages,
                    return_traces=ENDPOINT_SUPPORTS_FEEDBACK
                )
                response_area.empty()
                with response_area.container():
                    for message in messages:
                        render_message(message)
                return AssistantResponse(messages=messages, request_id=request_id)


    def query_responses_endpoint_and_render(input_messages):
        """Handle ResponsesAgent streaming format using MLflow types."""
        from mlflow.types.responses import ResponsesAgentStreamEvent
        
        with st.chat_message("assistant"):
            response_area = st.empty()
            response_area.markdown("_Thinking..._")
            
            # Track all the messages that need to be rendered in order
            all_messages = []
            request_id = None

            try:
                for raw_event in query_endpoint_stream(
                    endpoint_name=SERVING_ENDPOINT,
                    messages=input_messages,
                    return_traces=ENDPOINT_SUPPORTS_FEEDBACK
                ):
                    # Extract databricks_output for request_id
                    if "databricks_output" in raw_event:
                        req_id = raw_event["databricks_output"].get("databricks_request_id")
                        if req_id:
                            request_id = req_id
                    
                    # Parse using MLflow streaming event types, similar to ChatAgentChunk
                    if "type" in raw_event:
                        event = ResponsesAgentStreamEvent.model_validate(raw_event)
                        
                        if hasattr(event, 'item') and event.item:
                            item = event.item  # This is a dict, not a parsed object
                            
                            if item.get("type") == "message":
                                # Extract text content from message if present
                                content_parts = item.get("content", [])
                                for content_part in content_parts:
                                    if content_part.get("type") == "output_text":
                                        text = content_part.get("text", "")
                                        if text:
                                            all_messages.append({
                                                "role": "assistant",
                                                "content": text
                                            })
                                
                            elif item.get("type") == "function_call":
                                # Tool call
                                call_id = item.get("call_id")
                                function_name = item.get("name")
                                arguments = item.get("arguments", "")
                                
                                # Add to messages for history
                                all_messages.append({
                                    "role": "assistant",
                                    "content": "",
                                    "tool_calls": [{
                                        "id": call_id,
                                        "type": "function",
                                        "function": {
                                            "name": function_name,
                                            "arguments": arguments
                                        }
                                    }]
                                })
                                
                            elif item.get("type") == "function_call_output":
                                # Tool call output/result
                                call_id = item.get("call_id")
                                output = item.get("output", "")
                                
                                # Add to messages for history
                                all_messages.append({
                                    "role": "tool",
                                    "content": output,
                                    "tool_call_id": call_id
                                })
                    
                    # Update the display by rendering all accumulated messages
                    if all_messages:
                        with response_area.container():
                            for msg in all_messages:
                                render_message(msg)

                return AssistantResponse(messages=all_messages, request_id=request_id)
            except Exception:
                response_area.markdown("_Ran into an error. Retrying without streaming..._")
                messages, request_id = query_endpoint(
                    endpoint_name=SERVING_ENDPOINT,
                    messages=input_messages,
                    return_traces=ENDPOINT_SUPPORTS_FEEDBACK
                )
                response_area.empty()
                with response_area.container():
                    for message in messages:
                        render_message(message)
                return AssistantResponse(messages=messages, request_id=request_id)



    # --- Chat input (must run BEFORE rendering messages) ---
    prompt = st.chat_input("Ask the McCain Agent")
    if prompt:
        # Get the task type for this endpoint
        task_type = _get_endpoint_task_type(SERVING_ENDPOINT)
        
        # Add user message to chat history
        user_msg = UserMessage(content=prompt)
        st.session_state.history.append(user_msg)
        user_msg.render(len(st.session_state.history) - 1)

        # Convert history to standard chat message format for the query methods
        input_messages = [msg for elem in st.session_state.history for msg in elem.to_input_messages()]
        
        # Handle the response using the appropriate handler
        assistant_response = query_endpoint_and_render(task_type, input_messages)
        st.markdown(assistant_response)
        # Add assistant response to history
        st.session_state.history.append(assistant_response)

elif page == "Manufacturing Quality OEE Cost Analysis 🧱 ":
    st.title("Manufacturing Quality OEE Cost Analysis 🧱 ")
    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
    #dashboard_url = "https://e2-demo-field-eng.cloud.databricks.com/embed/dashboardsv3/01f0b5a6108d1b4899e595521bb31cae?o=1444828305810485"
    #components.iframe(dashboard_url, height=600, width=5000, scrolling=True)
    
    
    iframe_html = """
        <iframe
  src="https://e2-demo-field-eng.cloud.databricks.com/embed/dashboardsv3/01f0b9cf857a10449d2b54943a6528cb?o=1444828305810485"
  width="100%"
  height="1200"
  body.scrollTop = 0;
  frameborder="0">
  <style>
.stVerticalBlock {
    gap: 1rem !important; /* Adjust the gap as needed (0rem removes all space)
    body.scrollTop = 0; */
}</style>
</iframe>                  
                  """

    components.html(iframe_html, height=1200, scrolling=False)
    st.markdown("""
    <style>
    .stVerticalBlock {
    gap: 1rem !important; /* Adjust the gap as needed (0rem removes all space) */
    }
    .block-container {
        padding-top: 3rem;
        padding-bottom: 1rem;
    }
    </style>
    """, unsafe_allow_html=True)
    from databricks.sdk import WorkspaceClient
    import pandas as pd


    w = WorkspaceClient()
    genie_space_id = "01f0b9cf5c291e4cbc5e2de1c372cca1"

    import streamlit as st

    #if "messages" not in st.session_state:
     #   st.session_state.messages = []

    def display_message(message):
        if "content" in message:
            st.markdown(message["content"])
        if "data" in message:
            st.dataframe(message["data"])
        if "code" in message:
            with st.expander("Show generated code"):
                st.code(message["code"], language="sql", wrap_lines=True)
        
    def get_query_result(statement_id):
        result = w.statement_execution.get_statement(statement_id)
        return pd.DataFrame(
            result.result.data_array,
            columns=[i.name for i in result.manifest.schema.columns]
        )

    def process_genie_response(response):
        for i in response.attachments:
            if i.text:
                display_message({"role": "assistant", "content": i.text.content})
                st.session_state.messages.append({"role": "assistant", "content": i.text.content})
            elif i.query:
                data = get_query_result(response.query_result.statement_id)
                message = {
                    "role": "assistant",
                    "content": i.query.description,
                    "data": data,
                    "code": i.query.query
                }
                st.session_state.messages.append({"role": "assistant", "content": i.query.description,"data":data,"code":i.query.query})
                display_message(message) # added new 
    


    if prompt := st.chat_input("Ask Genie..."):
        st.chat_message("user").markdown(prompt)
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("assistant"):
            if st.session_state.get("conversation_id"):
                conversation = w.genie.create_message_and_wait(
                    genie_space_id, st.session_state.conversation_id, prompt
                )
            else:
                conversation = w.genie.start_conversation_and_wait(genie_space_id, prompt)
                process_genie_response(conversation)
                st.session_state.messages.append({"role": "user", "content": conversation})
                