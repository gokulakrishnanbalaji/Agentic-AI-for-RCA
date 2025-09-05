"""
Agentic AI for Root Cause Analysis - WORKING VERSION
Using LangGraph, ReAct Framework, and Gemini 1.5 Flash
"""

import os
import sqlite3
import pandas as pd
import json
import warnings
from datetime import datetime
import traceback
import re
import time
import random

# Import handling
try:
    from typing import Annotated
except ImportError:
    from typing_extensions import Annotated

from typing import Dict, List, Any, Optional, TypedDict

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Suppress warnings
warnings.filterwarnings("ignore", message="Unable to find acceptable character detection dependency")

# LangGraph and LangChain imports
from langgraph.graph import StateGraph, END, START
from langgraph.graph.message import add_messages
from langgraph.prebuilt import create_react_agent
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI

# Optional Langfuse (disabled for now to avoid auth issues)
LANGFUSE_ENABLED = False
print("⚠️ Langfuse disabled - running without tracing")

# State definition
class RootCauseState(TypedDict):
    messages: Annotated[list, add_messages]
    current_pattern: str
    investigation_checklist: List[str]
    completed_checks: List[str]
    findings: List[str]
    root_cause_found: bool
    iteration_count: int
    max_iterations: int
    available_columns: Dict[str, List[str]]
    execution_results: List[dict]
    final_conclusion: str

# Data Access Tools
class DataAccessTools:
    def __init__(self, metrics_folder: str, synthetic_db_path: str):
        self.metrics_folder = metrics_folder
        self.synthetic_db_path = synthetic_db_path
        self.csv_files = self._discover_csv_files()
        self.db_tables = self._discover_db_tables()
        
    def _discover_csv_files(self) -> Dict[str, str]:
        """Discover all CSV files in metrics folder"""
        csv_files = {}
        if os.path.exists(self.metrics_folder):
            for file in os.listdir(self.metrics_folder):
                if file.endswith('.csv'):
                    csv_files[file.replace('.csv', '')] = os.path.join(self.metrics_folder, file)
        return csv_files
    
    def _discover_db_tables(self) -> List[str]:
        """Discover all tables in synthetic database"""
        if not os.path.exists(self.synthetic_db_path):
            return []
        
        conn = sqlite3.connect(self.synthetic_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in cursor.fetchall()]
        conn.close()
        return tables
    
    def get_available_columns(self) -> Dict[str, List[str]]:
        """Get all available columns from CSV files and database tables"""
        columns = {}
        
        # CSV files
        for name, path in self.csv_files.items():
            try:
                df = pd.read_csv(path)
                columns[f"csv_{name}"] = df.columns.tolist()
            except Exception as e:
                print(f"Error reading CSV {name}: {e}")
        
        # Database tables
        if os.path.exists(self.synthetic_db_path):
            conn = sqlite3.connect(self.synthetic_db_path)
            for table in self.db_tables:
                try:
                    cursor = conn.cursor()
                    cursor.execute(f"PRAGMA table_info({table})")
                    table_columns = [col[1] for col in cursor.fetchall()]
                    columns[f"db_{table}"] = table_columns
                except Exception as e:
                    print(f"Error reading table {table}: {e}")
            conn.close()
        
        return columns
    
    def query_csv(self, file_name: str) -> pd.DataFrame:
        """Query CSV file"""
        if file_name in self.csv_files:
            return pd.read_csv(self.csv_files[file_name])
        else:
            available = list(self.csv_files.keys())
            raise ValueError(f"CSV file '{file_name}' not found. Available files: {available}")
    
    def query_database(self, sql_query: str) -> pd.DataFrame:
        """Execute SQL query on synthetic database"""
        if not os.path.exists(self.synthetic_db_path):
            raise ValueError("Database file not found")
            
        conn = sqlite3.connect(self.synthetic_db_path)
        try:
            result = pd.read_sql_query(sql_query, conn)
            return result
        finally:
            conn.close()
    
    def execute_pandas_code(self, code: str) -> Any:
        """Execute pandas code with available data"""
        context = {'pd': pd}
        
        # Load all CSV files into context
        for name, path in self.csv_files.items():
            try:
                context[name] = pd.read_csv(path)
            except Exception as e:
                print(f"Error loading {name}: {e}")
        
        # Add database query function to context
        context['query_db'] = self.query_database
        
        try:
            exec(code, context)
            return context.get('result', 'Code executed successfully')
        except Exception as e:
            return f"Error executing code: {str(e)}"

# FIXED: Simple tool definitions without complex decorators
def create_analysis_tools(data_tools: DataAccessTools):
    """Create analysis tools with proper docstrings"""
    
    @tool
    def query_csv_file(file_name: str) -> str:
        """Query CSV files from metrics folder by filename without extension and return data summary"""
        try:
            result = data_tools.query_csv(file_name)
            return f"CSV '{file_name}' - Shape: {result.shape}, Columns: {list(result.columns)}, Sample: {result.head(3).to_string()}"
        except Exception as e:
            return f"Error querying CSV '{file_name}': {str(e)}. Available files: {list(data_tools.csv_files.keys())}"
    
    @tool
    def execute_sql_query(sql_query: str) -> str:
        """Execute SQL query on synthetic database and return formatted results"""
        try:
            result = data_tools.query_database(sql_query)
            if result.empty:
                return f"SQL query executed successfully but returned no results. Query: {sql_query}"
            return f"SQL Result - Shape: {result.shape}, Sample: {result.head(5).to_string()}"
        except Exception as e:
            return f"SQL Error: {str(e)}. Available tables: {data_tools.db_tables}"
    
    @tool
    def execute_pandas_analysis(code: str) -> str:
        """Execute pandas code for data analysis with all CSV files preloaded and return results"""
        try:
            result = data_tools.execute_pandas_code(code)
            return f"Pandas analysis result: {str(result)}"
        except Exception as e:
            return f"Pandas error: {str(e)}. Available data: {list(data_tools.csv_files.keys())}"
    
    @tool
    def get_data_structure() -> str:
        """Get comprehensive overview of all available data sources and their column structure"""
        columns = data_tools.get_available_columns()
        info = "AVAILABLE DATA SOURCES:\n\n"
        
        # CSV Files
        csv_files = {k: v for k, v in columns.items() if k.startswith('csv_')}
        if csv_files:
            info += "CSV FILES:\n"
            for source, cols in csv_files.items():
                clean_name = source.replace('csv_', '')
                info += f"• {clean_name}: {len(cols)} columns - {', '.join(cols)}\n"
        
        # Database Tables
        db_tables = {k: v for k, v in columns.items() if k.startswith('db_')}
        if db_tables:
            info += "\nDATABASE TABLES:\n"
            for source, cols in db_tables.items():
                clean_name = source.replace('db_', '')
                info += f"• {clean_name}: {len(cols)} columns - {', '.join(cols)}\n"
        
        return info
    
    return [query_csv_file, execute_sql_query, execute_pandas_analysis, get_data_structure]

# Main analyzer class
class RootCauseAnalyzer:
    def __init__(self, metrics_folder: str, synthetic_db_path: str, gemini_api_key: str):
        self.data_tools = DataAccessTools(metrics_folder, synthetic_db_path)
        
        # Initialize Gemini LLM
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-1.5-flash",
            google_api_key=gemini_api_key,
            temperature=0.1
        )
        
        # Create tools
        self.tools = create_analysis_tools(self.data_tools)
        
        # Create system prompt
        system_prompt = ChatPromptTemplate.from_messages([
            ("system", "You are an expert data analyst specializing in root cause analysis. "
                      "Use the available tools systematically to investigate patterns and find root causes. "
                      "Always start with get_data_structure() to understand available data. "
                      "Be thorough and provide concrete evidence with actual data."),
            ("placeholder", "{messages}")
        ])
        
        # Create ReAct agent
        self.agent = create_react_agent(self.llm, self.tools, prompt=system_prompt)
        
        # Create workflow
        self.workflow = self._create_workflow()
    
    def _create_workflow(self) -> StateGraph:
        """Create the LangGraph workflow"""
        workflow = StateGraph(RootCauseState)
        
        # Add nodes
        workflow.add_node("initialize", self.initialize_analysis)
        workflow.add_node("detect_pattern", self.detect_pattern_node)
        workflow.add_node("generate_checklist", self.generate_checklist_node)
        workflow.add_node("investigate", self.investigate_node)
        workflow.add_node("evaluate", self.evaluate_findings_node)
        workflow.add_node("conclude", self.conclude_node)
        
        # Add edges
        workflow.add_edge(START, "initialize")
        workflow.add_edge("initialize", "detect_pattern")
        workflow.add_edge("detect_pattern", "generate_checklist")
        workflow.add_edge("generate_checklist", "investigate")
        workflow.add_edge("investigate", "evaluate")
        
        # Conditional edge
        workflow.add_conditional_edges(
            "evaluate",
            self.should_continue_investigation,
            {"continue": "investigate", "conclude": "conclude"}
        )
        workflow.add_edge("conclude", END)
        
        return workflow.compile()
    
    def print_workflow_graph(self):
        """Print the workflow graph structure"""
        print("📊 **WORKFLOW STRUCTURE**")
        print("🔧 NODES:", list(self.workflow.nodes.keys()))
        print("🔀 FLOW: START → initialize → detect_pattern → generate_checklist → investigate → evaluate → conclude → END")
    
    def initialize_analysis(self, state: RootCauseState) -> RootCauseState:
        """Initialize the analysis"""
        available_columns = self.data_tools.get_available_columns()
        state["available_columns"] = available_columns
        state["iteration_count"] = 0
        state["max_iterations"] = 3
        state["investigation_checklist"] = []
        state["completed_checks"] = []
        state["findings"] = []
        state["root_cause_found"] = False
        state["execution_results"] = []
        return state
    
    def detect_pattern_node(self, state: RootCauseState) -> RootCauseState:
        """Detect patterns using the agent"""
        user_query = state["messages"][-1].content if state["messages"] else "Analyze data patterns"
        
        pattern_message = f"""
TASK: Detect patterns based on user query: "{user_query}"

STEPS:
1. Use get_data_structure() to understand available data
2. Query relevant data sources
3. Identify specific patterns with concrete evidence
4. Provide data-backed observations

Be systematic and thorough.
"""
        
        try:
            response = self.agent.invoke({"messages": [HumanMessage(content=pattern_message)]})
            state["current_pattern"] = response["messages"][-1].content
            state["messages"].extend(response["messages"])
        except Exception as e:
            print(f"Pattern detection error: {e}")
            state["current_pattern"] = f"Pattern detection error: {str(e)}"
        
        return state
    
    def generate_checklist_node(self, state: RootCauseState) -> RootCauseState:
        """Generate investigation checklist"""
        try:
            checklist_message = f"""
Based on detected pattern: {state["current_pattern"]}

Generate exactly 3 specific investigation steps as JSON array:
["Step 1", "Step 2", "Step 3"]

Focus on:
- Temporal analysis
- External factors
- Data quality
"""
            
            response = self.llm.invoke([HumanMessage(content=checklist_message)])
            
            # Extract checklist
            json_match = re.search(r'\[(.*?)\]', response.content, re.DOTALL)
            if json_match:
                checklist_str = '[' + json_match.group(1) + ']'
                checklist = json.loads(checklist_str)
                checklist = [item.strip(' "\'') for item in checklist if item.strip()][:3]
            else:
                checklist = [
                    "Analyze temporal trends in revenue data",
                    "Investigate external factors and correlations", 
                    "Check for data quality issues and anomalies"
                ]
        except Exception as e:
            print(f"Checklist generation error: {e}")
            checklist = [
                "Analyze temporal trends in revenue data",
                "Investigate external factors and correlations", 
                "Check for data quality issues and anomalies"
            ]
        
        state["investigation_checklist"] = checklist
        return state
    
    def investigate_node(self, state: RootCauseState) -> RootCauseState:
        """Investigate next item in checklist"""
        # Find next uncompleted check
        next_item = None
        for item in state["investigation_checklist"]:
            if item not in state["completed_checks"]:
                next_item = item
                break
        
        if not next_item:
            return state
        
        investigation_message = f"""
INVESTIGATE: {next_item}

Context: {state["current_pattern"]}
Previous findings: {state["findings"]}

Use tools to gather concrete evidence for this investigation step.
"""
        
        try:
            response = self.agent.invoke({"messages": [HumanMessage(content=investigation_message)]})
            
            findings_text = response["messages"][-1].content
            state["findings"].append(f"{next_item}: {findings_text}")
            state["completed_checks"].append(next_item)
            state["messages"].extend(response["messages"])
            
        except Exception as e:
            print(f"Investigation error: {e}")
            state["findings"].append(f"{next_item}: Investigation failed - {str(e)}")
            state["completed_checks"].append(next_item)
        
        return state
    
    def evaluate_findings_node(self, state: RootCauseState) -> RootCauseState:
        """Evaluate current findings"""
        try:
            evaluation_message = f"""
Evaluate findings: {state["findings"]}

Determine if root cause is identified.
Respond: "ROOT_CAUSE_FOUND" or "CONTINUE_INVESTIGATION"
"""
            
            response = self.llm.invoke([HumanMessage(content=evaluation_message)])
            
            if "ROOT_CAUSE_FOUND" in response.content.upper():
                state["root_cause_found"] = True
                
        except Exception as e:
            print(f"Evaluation error: {e}")
        
        state["iteration_count"] = state.get("iteration_count", 0) + 1
        return state
    
    def should_continue_investigation(self, state: RootCauseState) -> str:
        """Determine whether to continue investigation"""
        if state.get("root_cause_found", False):
            return "conclude"
        if state.get("iteration_count", 0) >= state.get("max_iterations", 3):
            return "conclude"
        if len(state.get("completed_checks", [])) >= len(state.get("investigation_checklist", [])):
            return "conclude"
        return "continue"
    
    def conclude_node(self, state: RootCauseState) -> RootCauseState:
        """Generate final conclusion"""
        try:
            conclusion_message = f"""
Generate comprehensive root cause analysis conclusion:

Query: {state["messages"][0].content if state["messages"] else "Unknown"}
Pattern: {state["current_pattern"]}
Findings: {state["findings"]}

Provide:
1. Executive Summary
2. Root Cause Analysis
3. Key Evidence
4. Recommendations
"""
            
            response = self.llm.invoke([HumanMessage(content=conclusion_message)])
            state["final_conclusion"] = response.content
            
        except Exception as e:
            print(f"Conclusion error: {e}")
            state["final_conclusion"] = f"Analysis completed with {len(state['completed_checks'])} investigations."
        
        return state
    
    def analyze(self, user_query: str) -> Dict[str, Any]:
        """Run the complete analysis"""
        initial_state = RootCauseState(
            messages=[HumanMessage(content=user_query)],
            current_pattern="",
            investigation_checklist=[],
            completed_checks=[],
            findings=[],
            root_cause_found=False,
            iteration_count=0,
            max_iterations=3,
            available_columns={},
            execution_results=[],
            final_conclusion=""
        )
        
        result = self.workflow.invoke(initial_state)
        return result

# Main application class
class RootCauseApp:
    def __init__(self, metrics_folder: str, synthetic_db_path: str, gemini_api_key: str):
        self.analyzer = RootCauseAnalyzer(metrics_folder, synthetic_db_path, gemini_api_key)
    
    def analyze(self, query: str) -> str:
        """Run root cause analysis"""
        print(f"🔍 ROOT CAUSE ANALYSIS")
        print(f"Query: {query}")
        print("=" * 60)
        
        try:
            result = self.analyzer.analyze(query)
            
            print(f"🎯 Root Cause Found: {'✅' if result['root_cause_found'] else '❌'}")
            print(f"📋 Investigations: {len(result['completed_checks'])}/{len(result['investigation_checklist'])}")
            
            print("\n🔍 INVESTIGATION STEPS:")
            for i, item in enumerate(result['investigation_checklist'], 1):
                status = "✅" if item in result['completed_checks'] else "⏳"
                print(f"{i}. {status} {item}")
            
            print("\n📋 FINDINGS:")
            for i, finding in enumerate(result['findings'], 1):
                print(f"{i}. {finding[:200]}...")
            
            print("\n🎯 CONCLUSION:")
            print(result['final_conclusion'])
            
            return result['final_conclusion']
            
        except Exception as e:
            error_msg = f"Analysis failed: {str(e)}"
            print(f"❌ {error_msg}")
            return error_msg
    
    def get_data_overview(self) -> str:
        """Get overview of available data"""
        columns = self.analyzer.data_tools.get_available_columns()
        
        overview = "📊 DATA OVERVIEW\n\n"
        for source, cols in columns.items():
            overview += f"{source}: {len(cols)} columns\n"
            overview += f"  {', '.join(cols[:5])}{'...' if len(cols) > 5 else ''}\n\n"
        
        return overview

# Main execution
def main():
    print("🚀 ROOT CAUSE ANALYSIS SYSTEM")
    print("=" * 50)
    
    # Configuration
    METRICS_FOLDER = os.getenv("METRICS_FOLDER", "metrics")
    SYNTHETIC_DB_PATH = os.getenv("SYNTHETIC_DB_PATH", "synthetic_data/synthetic_data.db")
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    
    if not GEMINI_API_KEY:
        print("❌ GEMINI_API_KEY environment variable required!")
        return
    
    try:
        app = RootCauseApp(METRICS_FOLDER, SYNTHETIC_DB_PATH, GEMINI_API_KEY)
        
        # Show workflow structure
        app.analyzer.print_workflow_graph()
        
        # Show data overview
        print("\n" + app.get_data_overview())
        
        # Run analysis
        query = "Revenue has been declining in the last quarter, what could be the root cause?"
        result = app.analyze(query)
        
        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"rca_result_{timestamp}.txt"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"Root Cause Analysis Report\n")
            f.write(f"Generated: {datetime.now()}\n")
            f.write(f"Query: {query}\n\n")
            f.write(f"Result: {result}\n")
        print(f"💾 Results saved to {filename}")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
