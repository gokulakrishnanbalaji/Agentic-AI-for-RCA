import os

import pandas as pd

import json

import numpy as np

from typing import Dict, Any, List, Optional, TypedDict

from langchain_google_genai import ChatGoogleGenerativeAI

from langgraph.graph import StateGraph, END

import traceback

import glob

from datetime import datetime

from time import sleep

try:

    from scipy import stats

    SCIPY_AVAILABLE = True

except ImportError:

    SCIPY_AVAILABLE = False

class RCAState(TypedDict):

    iteration: int

    findings: Dict[str, Any]

    root_cause_found: bool

    next_actions: List[str]

    failure_timestamp: Optional[int]

    target_pods: List[str]

    pod_scores: Dict[str, float]

    root_cause_location: str

    has_data: bool

    primary_findings: List[str]

    terminate: bool

    performed_actions: List[Dict[str, Any]]

    current_action_index: int

    current_result: Any

def safe_json_dumps(obj, **kwargs):

    """Safely convert object to JSON string"""

    def convert(item):

        if isinstance(item, dict):

            return {str(k): convert(v) for k, v in item.items()}

        elif isinstance(item, list):

            return [convert(i) for i in item]

        elif isinstance(item, (np.integer, np.floating)):

            return float(item)

        elif isinstance(item, np.ndarray):

            return item.tolist()

        else:

            return str(item)

    try:

        return json.dumps(convert(obj), **kwargs)

    except:

        return str(obj)

# Dataset-specific hints for root cause analysis

DATASET_HINTS = {

    "20231207": '''Event: Processing of requests from some users is frequently delayed.

Cause: One of the duplicated pods in the load balancer is running, but a latent bug causes the cpu to rise, resulting in a slowdown in processing speed. As a result, latency increases for some users accessing the slow pod.

Difficulty in identifying the cause

Since the pod or service itself is not dead, autoscaling does not take place and is difficult to notice.

One POD is processing as usual, so there is no continuous effect on latency.

Since it does not appear in error rates, it is difficult to notice a failure without pinpointing and setting an alert for cpu utilization of individual pods, which may be similar to a silent failure.

Events that may be noticed by user reports.''',

    "20231221": '''

Event: The error rate of microserviceA increases.

Cause: A pod of microserviceB is moved to a different Node. However, an unexpected load increase occurs at the destination Node, affecting the pod of microserviceA on the same Node.

Difficulty in identifying the cause

The point that is NoisyNeigbor.

Before the failure occurs, the points are placed in different Nodes, and the failure occurs immediately after the Node move.

''',

    "20240115": '''

Event: Service error rate increases.

Cause: A container infected with malware attempts to login to other containers. The successfully logged-in container becomes a bot and tries to DDoS external services, which overwhelms the network. As a result, server errors of the service increase and the error rate rises.

''',

    "20240124": '''

Event: Service latency increases.

Cause: An application on the server froze and occupied the CPU. For example, if there were 4 CPUs, even if one of them was occupied, the CPU usage of the server being monitored was 25%, so it was not noticed.

''',

    "20240207": '''

Event: Affects the error rate of the service.

Cause: A worker deploys a manifest file with incorrect resource limits to the production environment. The system initially runs, but soon the resource limit is reached and the system is unable to handle all the user transactions, resulting in errors.

Difficulty in identifying the cause

Identify logs that are not tied to entities in the system. (e.g., logs of user operations)

Failure should not occur immediately after a Git Push.

''',

    "20240215": '''

Event: Cloud usage fees increase.

CAUSE: Cryptojacking downloads and installs Coin Miner before users knew it. The program gradually affects IT resources and increases the cost of cloud computing. (SLI is the cost of using the cloud computing)

Difficulties of root cause identification:

The cost of the entire system is SLI, so it is hard to know where the failure is occurring.

No errors were made in each service.

'''

}



class RCAAgent:

    def __init__(self, api_key: str, dataset: str, hint: str = ""):

        print("🤖 Initializing RCA Agent...")

        print("=" * 70)

        self.model = ChatGoogleGenerativeAI(

            model="gemini-2.5-flash",

            temperature=0,

            google_api_key=api_key

        )

        self.max_iterations = 5

        self.workflow = self._build_workflow()

        self.dataset_structure = ""

        self.dataset = dataset

        self.hint = hint

        self.file_paths = {}

        self.app = self.workflow.compile()

        print(f"✅ RCA Agent initialized successfully!")

        print(f"📋 Hint: {hint[:100]}..." if len(hint) > 100 else f"📋 Hint: {hint}\n")

    def _build_workflow(self):

        workflow = StateGraph(RCAState)

        workflow.add_node("load_data", self.load_data)

        workflow.add_node("learn_dataset_structure", self.learn_dataset_structure)

        workflow.add_node("analyse_KPI_files", self.analyse_KPI_files)

        workflow.add_node("create_actions", self.create_actions)

        workflow.add_node("perform_action", self.perform_action)

        workflow.add_node("analyse_result", self.analyse_result)

        workflow.add_node("check_for_root_cause", self.check_for_root_cause)

        workflow.add_node("summarise", self.summarise)

        workflow.set_entry_point("load_data")

        workflow.add_conditional_edges(

            "load_data",

            lambda state: state['has_data'],

            {True: "learn_dataset_structure", False: END}

        )

        workflow.add_edge("learn_dataset_structure", "analyse_KPI_files")

        workflow.add_edge("analyse_KPI_files", "create_actions")

        workflow.add_edge("create_actions", "perform_action")

        workflow.add_edge("perform_action", "analyse_result")

        workflow.add_edge("analyse_result", "check_for_root_cause")

        workflow.add_conditional_edges(

            "check_for_root_cause",

            lambda state: state["terminate"],

            {True: "summarise", False: "create_actions"}

        )

        workflow.add_edge('summarise', END)

        return workflow

    def load_data(self, state):

        print("\n🔍 STEP 1: LOADING DATA")

        print("=" * 70)

        curr_directories = os.listdir()

        if ('Log' in curr_directories) and ('Metrics' in curr_directories):

            state["has_data"] = True

            print("✅ Found Log and Metrics directories")

        else:

            state["has_data"] = False

            print("❌ Log or Metrics directories not found")

        state["iteration"] = 0

        state["findings"] = {}

        state["root_cause_found"] = False

        state["next_actions"] = []

        state["failure_timestamp"] = None

        state["target_pods"] = []

        state["pod_scores"] = {}

        state["root_cause_location"] = ""

        state["primary_findings"] = []

        state["terminate"] = False

        state["performed_actions"] = []

        state["current_action_index"] = 0

        print(f"📊 Initial state configured")

        return state

    def learn_dataset_structure(self, state):

        print("\n🔍 STEP 2: LEARNING DATASET STRUCTURE")

        print("=" * 70)

        dataset = self.dataset

        base_dirs = ["Log", "Metrics"]

        tree_output = []

        for base in base_dirs:

            folder_path = os.path.join(base, dataset)

            tree_output.append(f"{base}/")

            if not os.path.exists(folder_path) or not os.path.isdir(folder_path):

                tree_output.append(f"└── {dataset} [Not Found]")

                continue

            tree_output.append(f"└── {dataset}")

            prefix_stack = [(folder_path, "    ")]

            while prefix_stack:

                current_path, prefix = prefix_stack.pop()

                try:

                    entries = sorted(os.listdir(current_path))

                except PermissionError:

                    tree_output.append(prefix + "└── [Permission Denied]")

                    continue

                for i, entry in enumerate(entries):

                    full_path = os.path.join(current_path, entry)

                    connector = "└── " if i == len(entries) - 1 else "├── "

                    tree_output.append(prefix + connector + entry)

                    if os.path.isfile(full_path):

                        file_key = entry.replace('.csv', '').replace('.npy', '').replace('.npz', '')

                        self.file_paths[file_key] = full_path

                    if os.path.isdir(full_path):

                        new_prefix = prefix + ("    " if i == len(entries) - 1 else "│   ")

                        prefix_stack.append((full_path, new_prefix))

        self.dataset_structure = "\n".join(tree_output)

        print("📁 Dataset structure:")

        print(self.dataset_structure[:500] + "..." if len(self.dataset_structure) > 500 else self.dataset_structure)

        print(f"\n📂 Discovered {len(self.file_paths)} files")

        return state

    def analyse_KPI_files(self, state):

        """Analyze KPI files - LLM generates analysis code based on hint"""

        print("\n🔍 STEP 3: ANALYZING KPI FILES (HINT-DRIVEN)")

        print("=" * 70)

        print(f"📋 Using hint to guide analysis: {self.hint[:80]}...")

        try:

            metrics_path = os.path.join("Metrics", self.dataset)

            log_path = os.path.join("Log", self.dataset)

            primary_findings = []

            csv_files = []

            npy_files = []

            golden_signal_files = []

            # Get metrics files

            if os.path.exists(metrics_path):

                for root, dirs, files in os.walk(metrics_path):

                    for file in files:

                        if file.endswith('.csv'):

                            csv_files.append(os.path.join(root, file))

                        elif file.endswith('.npy') or file.endswith('.npz'):

                            npy_files.append(os.path.join(root, file))

            # Get golden signal files

            if os.path.exists(log_path):

                for root, dirs, files in os.walk(log_path):

                    for file in files:

                        if 'golden_signal' in file.lower() and (file.endswith('.npy') or file.endswith('.npz')):

                            golden_signal_files.append(os.path.join(root, file))

            print(f"📊 Found {len(csv_files)} CSV, {len(npy_files)} NPY, {len(golden_signal_files)} golden signal files")

            # Analyze golden signal files

            for golden_file in golden_signal_files[:2]:

                print(f"\n⭐ Analyzing GOLDEN SIGNAL: {os.path.relpath(golden_file)}")

                try:

                    golden_data = np.load(golden_file, allow_pickle=True).item()

                    if isinstance(golden_data, dict):

                        scenario_keys = list(golden_data.keys())

                        print(f"  Found {len(scenario_keys)} scenarios")

                        if scenario_keys:

                            first_scenario = scenario_keys[0]

                            scenario_data = golden_data[first_scenario]

                            if isinstance(scenario_data, dict):

                                data_keys = list(scenario_data.keys())

                                if 'Pod_Name' in data_keys:

                                    pods = scenario_data.get('Pod_Name', [])

                                    if isinstance(pods, list) and len(pods) > 0:

                                        unique_pods = list(set(pods))

                                        for pod in unique_pods[:10]:

                                            if pod not in state["target_pods"]:

                                                state["target_pods"].append(pod)

                                            if pod not in state["pod_scores"]:

                                                state["pod_scores"][pod] = 30.0

                                        finding = f"⭐ GOLDEN SIGNAL: {os.path.basename(golden_file)}\nPods: {unique_pods[:5]}\nTotal: {len(unique_pods)}\n"

                                        primary_findings.append(finding)

                                        print(f"  🎯 Extracted {len(unique_pods)} pods")

                except Exception as e:

                    print(f"  ⚠️ Error: {e}")

            # HINT-DRIVEN CSV ANALYSIS

            for csv_file in csv_files[:3]:

                print(f"\n🔥 Analyzing CSV (HINT-DRIVEN): {os.path.relpath(csv_file)}")

                try:

                    df = pd.read_csv(csv_file)

                    print(f"  Shape: {df.shape}, Columns: {list(df.columns)}")

                    stats_summary = df.describe().to_string()

                    sample_data = df.head(10).to_string()

                    # CHANGED: Ask LLM to generate analysis code based on hint

                    hint_analysis_prompt = f"""⚠️ CRITICAL: Generate Python code to analyze this data based on the provided HINT.

HINT (guides your analysis strategy):

{self.hint}

FILE: {os.path.basename(csv_file)}

COLUMNS: {list(df.columns)}

STATS: {stats_summary[:600]}

SAMPLE: {sample_data[:600]}

YOUR TASK:

1. Based on the HINT, determine what type of anomaly detection to use

2. Generate Python code that checks for anomalies relevant to the hint

3. Extract pod identifiers and score them based on hint-guided analysis

EXAMPLES OF HINT-DRIVEN ANALYSIS:

- Hint mentions "CPU": Check for CPU anomalies, correlate with performance

- Hint mentions "latency": Use statistical analysis on latency (mean + 3*std)

- Hint mentions "error rate": Look for error spikes

- Hint mentions "network": Check network-related metrics

- Hint mentions "resource limits": Look for resource exhaustion patterns

GENERATE PYTHON CODE (not JSON) that:

1. Loads the dataframe (already available as 'df')

2. Performs hint-specific anomaly detection

3. Returns context_hint string and affected_pods list

CODE TEMPLATE:

```python

# Hint-driven analysis

context_hint = ""

affected_pods = []

affected_pod_scores = {{}}

# Based on hint, detect specific anomalies

# Example for latency-related hint:

if 'Latency' in df.columns:

    # Statistical anomaly detection

    anomalies = df[df['Latency'] > df['Latency'].mean() + 3 * df['Latency'].std()]

    if len(anomalies) > 0:

        context_hint = "Latency anomalies detected using statistical analysis"

        if 'label' in df.columns:

            affected_pods = anomalies['label'].unique()[:5].tolist()

            # Score based on frequency and magnitude

            for pod in affected_pods:

                pod_data = anomalies[anomalies['label'] == pod]

                score = min(100, len(pod_data) * 10)

                affected_pod_scores[str(pod)] = float(score)

print("Context:", context_hint)

print("Affected pods:", affected_pods)

print("Scores:", affected_pod_scores)

```

OUTPUT: Only Python code that performs hint-guided analysis.

"""

                    # Get hint-driven analysis code from LLM

                    analysis_code_response = self.model.invoke(hint_analysis_prompt)

                    analysis_code = analysis_code_response.content.strip()

                    if analysis_code.startswith('```python'):

                        analysis_code = analysis_code.split('```python')[1].split('```')[0]

                    elif analysis_code.startswith('```'):

                        analysis_code = analysis_code.split('```')[1].split('```')[0]

                    # Execute the hint-driven analysis code

                    print(f"  📝 Executing hint-driven analysis code...")

                    try:

                        local_vars = {'df': df}

                        exec(analysis_code, {'pd': pd, 'np': np}, local_vars)

                        context_hint = local_vars.get('context_hint', '')

                        affected_pods = local_vars.get('affected_pods', [])

                        affected_pod_scores = local_vars.get('affected_pod_scores', {})

                        print(f"  ✅ Hint-driven analysis complete")

                        print(f"  📍 Context: {context_hint[:100]}")

                        print(f"  📍 Affected pods: {affected_pods[:3]}")

                        # Update state with hint-driven findings

                        for pod in affected_pods:

                            if str(pod) not in state["target_pods"]:

                                state["target_pods"].append(str(pod))

                        for pod, score in affected_pod_scores.items():

                            current_score = state["pod_scores"].get(str(pod), 0)

                            state["pod_scores"][str(pod)] = max(current_score, float(score))

                        # Update failure timestamp if available

                        if 'timeStamp' in df.columns and len(affected_pods) > 0:

                            # Get timestamp from anomalous data

                            if 'label' in df.columns:

                                anomaly_data = df[df['label'].isin(affected_pods)]

                                if len(anomaly_data) > 0:

                                    state["failure_timestamp"] = int(anomaly_data['timeStamp'].iloc[0])

                    except Exception as exec_error:

                        print(f"  ⚠️ Code execution error: {exec_error}")

                        context_hint = f"Analysis error: {str(exec_error)[:50]}"

                    # Now ask LLM to interpret results based on hint

                    interpret_prompt = f"""Based on the HINT and analysis results, provide pod rankings.

HINT: {self.hint}

FILE: {os.path.basename(csv_file)}

ANALYSIS RESULT:

- Context: {context_hint}

- Affected pods: {affected_pods[:5]}

- Pod scores: {affected_pod_scores}

OUTPUT JSON with pod rankings:

{{

  "pod_rankings": [{{"pod": "...", "anomaly_score": 0-100, "reason": "hint-related issue"}}],

  "hint_alignment": "how findings relate to hint"

}}

"""

                    interpret_response = self.model.invoke(interpret_prompt)

                    interpretation = interpret_response.content

                    try:

                        if '```json' in interpretation:

                            interpretation = interpretation.split('```json')[1].split('```')[0]

                        elif '```' in interpretation:

                            interpretation = interpretation.split('```')[1].split('```')[0]

                        interp_json = json.loads(interpretation.strip())

                        finding = f"\n📊 FILE: {os.path.basename(csv_file)}\n{context_hint}\n"

                        if interp_json.get('pod_rankings'):

                            for pod_info in interp_json.get('pod_rankings', [])[:5]:

                                pod_id = pod_info.get('pod', '')

                                score = pod_info.get('anomaly_score', 0)

                                reason = pod_info.get('reason', 'N/A')

                                if pod_id and pod_id not in state["target_pods"]:

                                    state["target_pods"].append(pod_id)

                                if pod_id:

                                    current_score = state["pod_scores"].get(pod_id, 0)

                                    state["pod_scores"][pod_id] = max(current_score, score)

                                finding += f"  📍 {pod_id}: Score={score}, {reason[:60]}\n"

                        if interp_json.get('hint_alignment'):

                            finding += f"  🎯 Hint alignment: {interp_json['hint_alignment'][:100]}\n"

                        primary_findings.append(finding)

                        print(finding[:300])

                    except json.JSONDecodeError:

                        finding = f"File: {os.path.basename(csv_file)}\n{context_hint}\n{interpretation[:150]}"

                        primary_findings.append(finding)

                        print(f"  ⚠️ JSON parsing failed")

                except Exception as e:

                    error_msg = f"Error: {str(e)}"

                    primary_findings.append(error_msg)

                    print(f"  ❌ {error_msg}")

            state["primary_findings"] = primary_findings

            if state["target_pods"]:

                print(f"\n🎯 Total pods identified: {len(state['target_pods'])} - {state['target_pods'][:5]}")

            if state["pod_scores"]:

                sorted_pods = sorted(state["pod_scores"].items(), key=lambda x: x[1], reverse=True)

                print(f"🏆 Top scored pods: {sorted_pods[:3]}")

            if state["failure_timestamp"]:

                print(f"⏰ Failure timestamp: {state['failure_timestamp']}")

            print(f"\n✅ Hint-driven analysis complete")

        except Exception as e:

            state["primary_findings"] = [f"Error: {str(e)}"]

            print(f"❌ Error: {e}")

        return state

    def create_actions(self, state):

        """Create actions based on hint"""

        print(f"\n🔍 STEP 4 (Iteration {state['iteration']}): CREATING HINT-DRIVEN ACTIONS")

        print("=" * 70)

        try:

            performed_context = ""

            if state.get("performed_actions"):

                performed_context = f"\nCompleted:\n"

                for action in state["performed_actions"][-2:]:

                    performed_context += f"- {action.get('action', 'Unknown')[:60]}...\n"

            known_context = f"""

DATASET: {self.dataset}

ITERATION: {state.get('iteration', 0)}

FAILURE_TIMESTAMP: {state.get('failure_timestamp', 'Unknown')}

IDENTIFIED_PODS: {state.get("target_pods", [])[:5]}

POD_SCORES: {dict(list(state.get("pod_scores", {}).items())[:3])}

"""

            # CHANGED: Emphasize hint in action creation

            prompt = f"""⚠️ CRITICAL: Create actions ALIGNED with the provided HINT.

HINT (guides investigation strategy):

{self.hint}

{known_context}

FINDINGS (based on hint-driven analysis):

{chr(10).join([f[:150] for f in state["primary_findings"][:3]])}

{performed_context}

YOUR TASK:

1. Create 2 analysis actions that are DIRECTLY RELEVANT to the hint

2. Actions should investigate aspects mentioned in the hint

3. Focus on identifying the SINGLE root cause location (pod/node)

EXAMPLES:

- If hint mentions "CPU": Create actions to analyze CPU metrics, correlate with pods

- If hint mentions "latency": Create actions to find pods with latency issues

- If hint mentions "network": Create actions to check network-related problems

- If hint mentions "resource limits": Create actions to check resource exhaustion

GOAL: Identify ONE specific pod/node that is the root cause location.

OUTPUT JSON (2 hint-aligned actions):

["Action 1 - based on hint", "Action 2 - confirm using hint context"]

"""

            response = self.model.invoke(prompt)

            try:

                response_content = response.content.strip()

                if '```json' in response_content:

                    response_content = response_content.split('```json')[1].split('```')[0]

                elif '```' in response_content:

                    response_content = response_content.split('```')[1].split('```')[0]

                actions = json.loads(response_content.strip())

                state["next_actions"] = actions[:2]

                state["current_action_index"] = 0

                print("📋 Hint-driven actions:")

                for i, action in enumerate(state["next_actions"], 1):

                    print(f"  {i}. {action[:100]}...")

            except json.JSONDecodeError:

                print(f"⚠️ Fallback actions")

                state["next_actions"] = [

                    f"Analyze metrics guided by hint: {self.hint[:50]}...",

                    "Confirm top-ranked pod as root cause location"

                ]

                state["current_action_index"] = 0

        except Exception as e:

            print(f"❌ Error: {e}")

            state["next_actions"] = ["Identify root cause location pod"]

            state["current_action_index"] = 0

        return state

    def perform_action(self, state):

        """Execute action - pass hint to code generation"""

        print(f"\n🔍 STEP 5 (Iteration {state['iteration']}): PERFORMING ACTION")

        print("=" * 70)

        try:

            if not state["next_actions"] or state["current_action_index"] >= len(state["next_actions"]):

                state["current_result"] = {"action": "No more", "result": {}, "success": False}

                print("⚠️ No more actions")

                return state

            current_action = state["next_actions"][state["current_action_index"]]

            print(f"🎯 Action: {current_action[:120]}...")

            # CHANGED: Pass hint to code generation

            prompt = f"""⚠️ Generate Python code ALIGNED with the HINT.

HINT (guides your code):

{self.hint}

ACTION: {current_action}

CONTEXT:

- Dataset: {self.dataset}

- Known pods: {state.get('target_pods', [])[:5]}

- Current scores: {dict(list(state.get('pod_scores', {}).items())[:3])}

GOAL: Identify the SINGLE most problematic pod/node based on hint-guided analysis.

CODE MUST:

1. Use hint to determine what metrics/patterns to check

2. Analyze data sources relevant to hint

3. Return ONE top-ranked pod

CODE TEMPLATE:

```python

import glob, numpy as np, pandas as pd

analysis_result = {{

    "all_pods": [],

    "pod_scores": {{}},

    "root_cause_location": "",

    "confidence": 0,

    "hint_evidence": ""

}}

# HINT-GUIDED ANALYSIS

# Example: If hint mentions latency, focus on latency metrics

# Example: If hint mentions CPU, focus on CPU metrics

# Try golden signal first

golden_files = glob.glob('Log/{self.dataset}/**/*golden_signal*.npy', recursive=True)

if golden_files:

    try:

        data = np.load(golden_files[0], allow_pickle=True).item()

        scenario = data[list(data.keys())[0]]

        pods = list(scenario.get('Pod_Name', []))

        

        if pods:

            unique_pods = list(set(pods))

            analysis_result["all_pods"] = [str(p) for p in unique_pods[:10]]

            

            # Score based on hint (e.g., frequency if hint mentions specific pattern)

            for pod in unique_pods:

                count = pods.count(pod)

                analysis_result["pod_scores"][str(pod)] = count

            

            # Get top pod

            if analysis_result["pod_scores"]:

                top_pod = max(analysis_result["pod_scores"].items(), key=lambda x: x[1])

                analysis_result["root_cause_location"] = top_pod[0]

                analysis_result["confidence"] = 80

                analysis_result["hint_evidence"] = "Highest frequency in golden signal data"

                

            print("Golden signal:", analysis_result["root_cause_location"])

    except Exception as e:

        print(f"Error: {{e}}")

# Try CSV metrics with hint-specific analysis

if not analysis_result["root_cause_location"]:

    csv_files = glob.glob('Metrics/{self.dataset}/**/*.csv', recursive=True)

    if csv_files:

        try:

            df = pd.read_csv(csv_files[0], nrows=2000)

            

            # HINT-DRIVEN METRIC SELECTION

            # Adapt based on columns and hint

            if 'label' in df.columns and 'Latency' in df.columns:

                # Statistical anomaly detection

                pod_stats = df.groupby('label')['Latency'].agg(['mean', 'std', 'max'])

                pod_stats['anomaly_score'] = pod_stats['max']

                

                top_pod = pod_stats['anomaly_score'].idxmax()

                analysis_result["root_cause_location"] = str(top_pod)

                analysis_result["pod_scores"] = {{str(k): float(v) for k, v in pod_stats['anomaly_score'].items()}}

                analysis_result["all_pods"] = list(pod_stats.index.astype(str))

                analysis_result["confidence"] = 70

                analysis_result["hint_evidence"] = "Highest latency anomaly score"

                

                print("CSV analysis:", analysis_result["root_cause_location"])

        except Exception as e:

            print(f"Error: {{e}}")

print("Result:", analysis_result)

```

OUTPUT: Only Python code for hint-guided analysis.

"""

            response = self.model.invoke(prompt)

            code = response.content.strip()

            if code.startswith('```python'):

                code = code.split('```python')[1].split('```')[0]

            elif code.startswith('```'):

                code = code.split('```')[1].split('```')[0]

            # Execute with retry loop

            max_retries = 3

            retry_count = 0

            execution_successful = False

            last_error = None

            while retry_count < max_retries and not execution_successful:

                try:

                    print(f"📝 Executing code (attempt {retry_count + 1}/{max_retries})...")

                    local_vars = {}

                    global_vars = {

                        'pd': pd, 'np': np, 'os': os, 'glob': glob, 'json': json, 'print': print

                    }

                    if SCIPY_AVAILABLE:

                        global_vars['stats'] = stats

                    exec(code, global_vars, local_vars)

                    result = local_vars.get('analysis_result', {'root_cause_location': '', 'confidence': 0})

                    print(f"✅ Success: {str(result)[:150]}...")

                    state["current_result"] = {

                        "action": current_action,

                        "result": result,

                        "success": True

                    }

                    execution_successful = True

                except Exception as e:

                    last_error = str(e)

                    retry_count += 1

                    print(f"❌ Error (attempt {retry_count}/{max_retries}): {last_error[:100]}")

                    if retry_count < max_retries:

                        print(f"🔄 Asking LLM to fix code...")

                        fix_prompt = f"""Fix this code. Error: {last_error}

HINT: {self.hint}

FAILED CODE:

```python

{code}

```

Generate ONLY corrected Python code:

"""

                        try:

                            fix_response = self.model.invoke(fix_prompt)

                            code = fix_response.content.strip()

                            if code.startswith('```python'):

                                code = code.split('```python')[1].split('```')[0]

                            elif code.startswith('```'):

                                code = code.split('```')[1].split('```')[0]

                            print(f"✓ Received fixed code")

                        except Exception as fix_error:

                            print(f"⚠️ Could not get fixed code: {fix_error}")

                            break

                    else:

                        print(f"❌ Max retries reached")

                        state["current_result"] = {

                            "action": current_action,

                            "result": {'root_cause_location': '', 'confidence': 0},

                            "error": last_error[:150],

                            "success": False

                        }

        except Exception as e:

            print(f"❌ Error: {e}")

            state["current_result"] = {"action": "Unknown", "result": {}, "success": False}

        return state

    def analyse_result(self, state):

        print(f"\n🔍 STEP 6 (Iteration {state['iteration']}): ANALYZING RESULT")

        print("=" * 70)

        try:

            current_result = state.get("current_result", {})

            result = current_result.get('result', {})

            if isinstance(result, dict):

                root_location = result.get('root_cause_location', '')

                if root_location and root_location not in state["target_pods"]:

                    state["target_pods"].append(root_location)

                    print(f"🎯 Root cause location found: {root_location}")

                pod_scores = result.get('pod_scores', {})

                for pod, score in pod_scores.items():

                    if pod not in state["pod_scores"] or score > state["pod_scores"].get(pod, 0):

                        state["pod_scores"][pod] = float(score)

                all_pods = result.get('all_pods', [])

                for pod in all_pods:

                    if pod not in state["target_pods"]:

                        state["target_pods"].append(pod)

            action_analysis = {

                "action": current_result.get('action', 'Unknown')[:150],

                "result": str(result)[:300],

                "iteration": state["iteration"],

                "success": current_result.get('success', False)

            }

            state["performed_actions"].append(action_analysis)

            state["current_action_index"] += 1

        except Exception as e:

            print(f"❌ Error: {e}")

            state["performed_actions"].append({"action": "Error", "iteration": state["iteration"]})

        return state

    def check_for_root_cause(self, state):

        print(f"\n🔍 STEP 7: CHECKING ROOT CAUSE LOCATION")

        print("=" * 70)

        print(f"  Iteration: {state.get('iteration', 0)}, Pods: {len(state.get('target_pods', []))}, Scores: {len(state.get('pod_scores', {}))}")

        try:

            if state.get("pod_scores") and state.get('iteration', 0) >= 1:

                sorted_pods = sorted(state["pod_scores"].items(), key=lambda x: x[1], reverse=True)

                top_pod = sorted_pods[0][0]

                top_score = sorted_pods[0][1]

                

                state["root_cause_found"] = True

                state["terminate"] = True

                state["root_cause_location"] = top_pod

                

                print(f"🎉 Root cause LOCATION: {top_pod} (score: {top_score})")

            elif state.get("target_pods") and state.get('iteration', 0) >= 1:

                state["root_cause_found"] = True

                state["terminate"] = True

                state["root_cause_location"] = state["target_pods"][0]

                

                print(f"🎉 Root cause LOCATION: {state['root_cause_location']} (first identified)")

            elif state["iteration"] + 1 >= self.max_iterations:

                state["terminate"] = True

                if state.get('pod_scores'):

                    sorted_pods = sorted(state["pod_scores"].items(), key=lambda x: x[1], reverse=True)

                    state["root_cause_location"] = sorted_pods[0][0]

                    state["root_cause_found"] = True

                elif state.get('target_pods'):

                    state["root_cause_location"] = state["target_pods"][0]

                    state["root_cause_found"] = True

                else:

                    state["root_cause_location"] = "unknown"

                print(f"⚠️ Max iterations - Location: {state['root_cause_location']}")

            else:

                state["iteration"] += 1

                print(f"➡️ Next iteration: {state['iteration']}")

        except Exception as e:

            print(f"❌ Error: {e}")

            state["terminate"] = True

            state["root_cause_location"] = "error"

        return state

    def summarise(self, state):

        """Create detailed summary"""

        print(f"\n🔍 STEP 8: SUMMARY OF ROOT CAUSE LOCATION")

        print("=" * 70)

        try:

            if not os.path.exists("results"):

                os.makedirs("results")

            sorted_pods_display = ""

            if state.get("pod_scores"):

                sorted_pods = sorted(state["pod_scores"].items(), key=lambda x: x[1], reverse=True)

                sorted_pods_display = "\n".join([f"  {i+1}. {pod}: {score:.2f}" for i, (pod, score) in enumerate(sorted_pods[:5])])

            summary_content = f"""{'='*80}

ROOT CAUSE LOCATION ANALYSIS SUMMARY (HINT-DRIVEN)

{'='*80}

ANALYSIS METADATA

Dataset: {self.dataset}

Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Iterations: {state.get('iteration', 0)}/{self.max_iterations}

Actions Performed: {len(state.get('performed_actions', []))}

Location Found: {state.get('root_cause_found', False)}

HINT (Guiding Analysis):

{self.hint}

ROOT CAUSE LOCATION (SINGLE POD/NODE)

================================================================================

Status: {"✅ IDENTIFIED" if state.get('root_cause_found') else "❌ NOT FOUND"}

Location: {state.get('root_cause_location', 'Not identified')}

Failure Timestamp: {state.get('failure_timestamp', 'Not identified')}
{pd.to_datetime(state.get('failure_timestamp', 00000000000), unit='s')
}

POD RANKINGS (HINT-DRIVEN)

================================================================================

{sorted_pods_display if sorted_pods_display else "No pod scores available"}

ALL IDENTIFIED PODS ({len(state.get('target_pods', []))}):

{', '.join(state.get('target_pods', [])[:20])}

PRIMARY FINDINGS (HINT-BASED)

================================================================================

{chr(10).join([f"{i+1}. {str(f)[:250]}" for i, f in enumerate(state.get('primary_findings', [])[:5])])}

ACTIONS PERFORMED

================================================================================

"""

            for i, action in enumerate(state.get('performed_actions', []), 1):

                success = "✅" if action.get('success') else "❌"

                summary_content += f"\n{i}. {success} {action.get('action', 'Unknown')[:150]}\n"

            summary_content += f"\n{'='*80}\nEND OF LOCATION REPORT\n{'='*80}\n"

            summary_filename = f"results/location_summary_{self.dataset}.txt"

            with open(summary_filename, 'w', encoding='utf-8') as f:

                f.write(summary_content)

            print(f"✅ Saved: {summary_filename}")

            print(f"📍 ROOT CAUSE LOCATION: {state.get('root_cause_location')}")

        except Exception as e:

            print(f"❌ Error: {e}")

        return state

    def run_analysis(self, initial_state=None):

        print("\n" + "=" * 70)

        print("🚀 STARTING HINT-DRIVEN ROOT CAUSE LOCATION ANALYSIS")

        print("=" * 70)

        if initial_state is None:

            initial_state = {}

        try:

            result = self.app.invoke(initial_state)

            print("\n" + "=" * 70)

            print("🎉 LOCATION ANALYSIS COMPLETE!")

            print("=" * 70)

            return result

        except Exception as e:

            print(f"❌ Error: {e}")

            traceback.print_exc()

            return {"error": str(e)}

# Multi-dataset runner

if __name__ == "__main__":

    datasets = ["20231207", "20231221", "20240115", "20240124", "20240207", "20240215"]

    for dataset in datasets:

        print("\n" + "="*80)

        print(f"PROCESSING DATASET: {dataset}")

        print("="*80)

        try:

            hint = DATASET_HINTS.get(dataset, "")

            agent = RCAAgent(api_key="AIzaSyA6qIGX-eUG_kaRO26q9SgRYcvkMg10YjA", dataset=dataset, hint=hint)

            result = agent.run_analysis()

            print(f"\n✅ Completed dataset {dataset}")

            print(f"ROOT CAUSE LOCATION: {result.get('root_cause_location', 'Unknown')}")

        except Exception as e:

            print(f"\n❌ Error processing dataset {dataset}: {e}")

        print(f"\n💤 Sleeping 60 seconds before next dataset...")

        sleep(60)

    print("\n" + "="*80)

    print("🎉 ALL DATASETS PROCESSED!")

    print("="*80)
