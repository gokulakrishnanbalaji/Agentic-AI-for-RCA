import os
import pandas as pd
import numpy as np
import glob
import sys
import io
from typing import Dict, List, Any, TypedDict
from dotenv import load_dotenv
from langgraph.graph import StateGraph, END
import google.generativeai as genai
from langchain_google_genai import ChatGoogleGenerativeAI
import matplotlib.pyplot as plt
import seaborn as sns

load_dotenv()
genai.configure(api_key=os.getenv('GEMINI_API_KEY'))

class RootCauseState(TypedDict):
    question: str
    current_findings: str
    iteration_count: int
    max_iterations: int
    root_cause_found: bool
    root_cause: str
    primary_data_insights: List[str]
    secondary_data_insights: List[str]
    analysis_path: List[str]
    executed_code_results: List[str]

class RootCauseAgent:
    def __init__(self):
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash",
            google_api_key=os.getenv('GEMINI_API_KEY'),
            temperature=0.1
        )
        self.metrics_path = "metrics/"
        self.synthetic_path = "synthetic_data/"
        self.primary_data = {}
        self.secondary_data = {}
        
    def load_primary_data(self) -> Dict[str, pd.DataFrame]:
        if not self.primary_data:
            csv_files = glob.glob(os.path.join(self.metrics_path, "*.csv"))
            for file in csv_files:
                filename = os.path.basename(file).replace('.csv', '')
                try:
                    self.primary_data[filename] = pd.read_csv(file)
                except Exception as e:
                    print(f"Error loading {file}: {e}")
        return self.primary_data
    
    def load_secondary_data(self) -> Dict[str, pd.DataFrame]:
        if not self.secondary_data:
            csv_files = glob.glob(os.path.join(self.synthetic_path, "*.csv"))
            for file in csv_files:
                filename = os.path.basename(file).replace('.csv', '')
                try:
                    self.secondary_data[filename] = pd.read_csv(file)
                except Exception as e:
                    print(f"Error loading {file}: {e}")
        return self.secondary_data

    def get_clean_dataframe_info(self, data_context: Dict[str, pd.DataFrame]) -> str:
        """Get clean, essential dataframe information"""
        info_lines = []
        for name, df in data_context.items():
            date_cols = [col for col in df.columns if 'date' in col.lower() or 'start' in col.lower()]
            info_lines.append(f"{name}: {df.shape[0]} rows, {df.shape[1]} cols")
            if date_cols:
                info_lines.append(f"  Date columns: {date_cols}")
            info_lines.append(f"  Key columns: {list(df.columns)}")
        return '\n'.join(info_lines)

    def execute_pandas_code(self, code: str, data_context: Dict[str, pd.DataFrame]) -> str:
        exec_globals = {'pd': pd, 'np': np, 'plt':plt, 'sns':sns}
        exec_globals.update(data_context)
        
        old_stdout = sys.stdout
        sys.stdout = captured_output = io.StringIO()
        
        try:
            exec(code, exec_globals)
            output = captured_output.getvalue()
            return output.strip() if output else "No output generated"
        except Exception as e:
            return f"ERROR: {str(e)}"
        finally:
            sys.stdout = old_stdout

def identify_pattern_node(state: RootCauseState) -> RootCauseState:
    print("\n" + "="*50)
    print("🔍 STEP 1: PATTERN IDENTIFICATION")
    print("="*50)
    
    agent = RootCauseAgent()
    primary_data = agent.load_primary_data()
    
    if not primary_data:
        state['current_findings'] = "No data found"
        return state
    
    data_info = agent.get_clean_dataframe_info(primary_data)
    print(f"Data loaded:\n{data_info}")
    
    pattern_prompt = f"""
    Question: {state['question']}
    
    Data available: {data_info}
    
    Generate clean pandas code to analyze revenue from 2011 Nov to 2012 Jan.
    Handle date conversions if needed. Use print() for output.
    
    CODE:
    ```
    # Clean analysis code here
    ```
    """
    
    response = agent.llm.invoke(pattern_prompt)
    response_content = response.content
    
    if "```python" in response_content:
        code_start = response_content.find("```python") + 9
        code_end = response_content.find("```", code_start)
        code = response.content[code_start:code_end].strip()
        
        # Remove imports
        code_lines = [line for line in code.split('\n') 
                     if not line.strip().startswith('import ') 
                     and not line.strip().startswith('from ')]
        cleaned_code = '\n'.join(code_lines)
        
        print("\n📋 Executing Analysis Code:")
        print("-" * 30)
        result = agent.execute_pandas_code(cleaned_code, primary_data)
        print(result)
        
        state['executed_code_results'].append(result)
    
    state['analysis_path'].append("Pattern Analysis")
    state['iteration_count'] = 1
    return state

def analyze_primary_data_node(state: RootCauseState) -> RootCauseState:
    print("\n" + "="*50)
    print("📊 STEP 2: PRIMARY DATA DEEP DIVE")
    print("="*50)
    
    agent = RootCauseAgent()
    primary_data = agent.load_primary_data()
    
    prompt = f"""
    Question: {state['question']}
    Previous results: {state['executed_code_results'][-1] if state['executed_code_results'] else 'None'}
    
    Generate code for detailed trend analysis, month-over-month changes, and insights.
    Focus on Nov 2011, Dec 2011, Jan 2012 patterns.
    
    CODE:
    ```python
    # Detailed trend analysis
    ```
    """
    
    response = agent.llm.invoke(prompt)
    
    response_content = response.content
    
    if "```python" in response_content:
        code_start = response_content.find("```python") + 9
        code_end = response_content.find("```", code_start)
        code = response.content[code_start:code_end].strip()
        
        code_lines = [line for line in code.split('\n') 
                     if not line.strip().startswith('import ') 
                     and not line.strip().startswith('from ')]
        cleaned_code = '\n'.join(code_lines)
        
        print("\n📋 Executing Detailed Analysis:")
        print("-" * 30)
        result = agent.execute_pandas_code(cleaned_code, primary_data)
        print(result)
        
        state['executed_code_results'].append(result)
        state['primary_data_insights'].append(result)
    
    state['analysis_path'].append("Primary Analysis")
    return state

def analyze_secondary_data_node(state: RootCauseState) -> RootCauseState:
    print("\n" + "="*50)
    print(f"🔬 STEP 3: ROOT CAUSE INVESTIGATION (Iteration {state['iteration_count']})")
    print("="*50)
    
    agent = RootCauseAgent()
    primary_data = agent.load_primary_data()
    secondary_data = agent.load_secondary_data()
    all_data = {**primary_data, **secondary_data}
    
    prompt = f"""
    Question: {state['question']}
    
    Primary findings: {state['primary_data_insights'][-1] if state['primary_data_insights'] else 'None'}
    
    Generate focused code to find root causes using secondary data:
    - Marketing spend and campaigns
    - External factors (holidays, economic conditions)
    - Customer behavior changes
    - Competitor activities
    
    Keep code simple and focused on key insights.
    
    CODE:
    ```
    # Root cause analysis - focus on key drivers
    ```
    """
    
    response = agent.llm.invoke(prompt)
    
    response_content = response.content
    
    if "```python" in response_content:
        code_start = response_content.find("```python") + 9
        code_end = response_content.find("```", code_start)
        code = response.content[code_start:code_end].strip()
        
        # Fix common code errors
        code = code.replace('cb_cb[', 'cb_monthly[')
        code = code.replace('so_so[', 'so_monthly[')
        
        code_lines = [line for line in code.split('\n') 
                     if not line.strip().startswith('import ') 
                     and not line.strip().startswith('from ')]
        cleaned_code = '\n'.join(code_lines)
        
        print("\n📋 Executing Root Cause Analysis:")
        print("-" * 30)
        result = agent.execute_pandas_code(cleaned_code, all_data)
        print(result)
        
        state['executed_code_results'].append(result)
        state['secondary_data_insights'].append(result)
    
    state['analysis_path'].append(f"Secondary Analysis (Iter {state['iteration_count']})")
    return state

def root_cause_evaluation_node(state: RootCauseState) -> RootCauseState:
    print("\n" + "="*50)
    print("🎯 STEP 4: ROOT CAUSE EVALUATION")
    print("="*50)
    
    agent = RootCauseAgent()
    
    # Check if we have meaningful results
    has_revenue_data = any("revenue" in result.lower() or "sales" in result.lower() 
                          for result in state['executed_code_results'])
    has_secondary_insights = len(state['secondary_data_insights']) > 0
    
    evaluation_prompt = f"""
    Question: {state['question']}
    
    Analysis Results Summary:
    {chr(10).join(state['executed_code_results'])}
    
    Based on the data analysis:
    1. Do we have clear revenue trends for Nov 2011 - Jan 2012?
    2. Have we identified specific factors causing the revenue changes?
    3. Can we provide actionable insights?
    
    If we have sufficient insights, respond with:
    "ROOT_CAUSE_FOUND: [Clear summary of findings]"
    
    If more analysis needed:
    "CONTINUE_ANALYSIS: [What to investigate next]"
    """
    
    response = agent.llm.invoke(evaluation_prompt)
    response_text = response.content
    
    print("🔍 Evaluation Result:")
    print(response_text)
    
    # Improved root cause detection
    if ("ROOT_CAUSE_FOUND:" in response_text or 
        (has_revenue_data and has_secondary_insights and state['iteration_count'] >= 2)):
        state['root_cause_found'] = True
        if "ROOT_CAUSE_FOUND:" in response_text:
            state['root_cause'] = response_text.replace("ROOT_CAUSE_FOUND:", "").strip()
        else:
            state['root_cause'] = "Revenue analysis completed with supporting secondary data insights"
        print("✅ ROOT CAUSE IDENTIFIED!")
    else:
        state['root_cause_found'] = False
        print("🔄 Continuing analysis...")
    
    state['iteration_count'] += 1
    return state

def should_continue(state: RootCauseState) -> str:
    if state['root_cause_found']:
        return "end"
    elif state['iteration_count'] >= state['max_iterations']:
        return "end"
    else:
        return "continue"

def create_root_cause_workflow():
    workflow = StateGraph(RootCauseState)
    
    workflow.add_node("identify_pattern", identify_pattern_node)
    workflow.add_node("analyze_primary", analyze_primary_data_node)
    workflow.add_node("analyze_secondary", analyze_secondary_data_node)
    workflow.add_node("evaluate_root_cause", root_cause_evaluation_node)
    
    workflow.set_entry_point("identify_pattern")
    workflow.add_edge("identify_pattern", "analyze_primary")
    workflow.add_edge("analyze_primary", "analyze_secondary")
    workflow.add_edge("analyze_secondary", "evaluate_root_cause")
    
    workflow.add_conditional_edges(
        "evaluate_root_cause",
        should_continue,
        {"continue": "analyze_secondary", "end": END}
    )
    
    return workflow.compile()

def run_root_cause_analysis(question: str, max_iterations: int = 3):
    print("🚀 ROOT CAUSE ANALYSIS STARTING")
    print(f"Question: {question}")
    
    initial_state = RootCauseState(
        question=question,
        current_findings="",
        iteration_count=0,
        max_iterations=max_iterations,
        root_cause_found=False,
        root_cause="",
        primary_data_insights=[],
        secondary_data_insights=[],
        analysis_path=[],
        executed_code_results=[]
    )
    
    workflow = create_root_cause_workflow()
    result = workflow.invoke(initial_state)
    
    # Clean final output
    print("\n" + "="*60)
    print("🏁 FINAL RESULTS")
    print("="*60)
    print(f"❓ Question: {result['question']}")
    print(f"🛤️  Analysis Path: {' → '.join(result['analysis_path'])}")
    print(f"🔢 Iterations: {result['iteration_count']}")
    print(f"✅ Root Cause Found: {result['root_cause_found']}")
    
    if result['root_cause']:
        print(f"\n🎯 ROOT CAUSE:\n{result['root_cause']}")
    
    print(f"\n📊 KEY INSIGHTS:")
    for i, result_text in enumerate(result['executed_code_results'], 1):
        if result_text and "ERROR" not in result_text:
            print(f"\n{i}. {result_text[:300]}{'...' if len(result_text) > 300 else ''}")
    
    print("="*60)
    return result

if __name__ == "__main__":
    question = "How is the revenue from 2011 Nov to 2012 Jan?"
    print(run_root_cause_analysis(question, max_iterations=3))
