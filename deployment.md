This project has two main parts that need to run in AWS:
1.  **The DQ Check Runner (`run_automatic_dq_checks.py`):** This is a scheduled script that connects to Snowflake, runs checks, and produces a JSON report.
2.  **The DQ Dashboard (`dq_dashboard.py`):** This is a Streamlit web application that visualizes the JSON report.

## AWS Deployment Guide: Snowflake Data Quality Platform

### 1. High-Level Architecture Overview

Before we dive into the steps, let's visualize our target architecture:

*   **For the DQ Check Runner (`run_automatic_dq_checks.py`):** We'll use **AWS Lambda** triggered by **Amazon EventBridge (formerly CloudWatch Events)**. Lambda is perfect for scheduled, short-lived tasks. It's serverless, so you only pay for compute time.
*   **For the DQ Dashboard (`dq_dashboard.py`):** We'll deploy it using **AWS App Runner**. This service makes it easy to deploy containerized web applications directly from source code or a container image, handling scaling, load balancing, and SSL automatically. It's ideal for Streamlit apps.
*   **For Storing DQ Reports (`dq_results.json`):** We'll use **Amazon S3**. This is highly durable, scalable, and cheap object storage. Our Lambda function will write the JSON results here, and our App Runner application will read them from here.
*   **For Snowflake Credentials:** We'll use **AWS Secrets Manager**. This is the most secure way to store and retrieve sensitive information, preventing hardcoding credentials in your application code or environment variables.
*   **Permissions:** We'll define granular permissions using **IAM Roles** for our Lambda function and App Runner service.

```
+-----------------+     +-----------------+     +-------------------+
|     Lambda      | <---|   EventBridge   |     |    AWS Secrets    |
| (DQ Check Run)  |     |   (Scheduler)   | <---|      Manager    |
+--------+--------+     +-----------------+     +-------------------+
         |                                                 ^
         | Writes JSON Report                              | Reads JSON Report
         v                                                 |
+--------+--------+                                  +-----+------+
|      Amazon S3  | <--------------------------------| AWS App Runner |
| (DQ Reports)    |                                  | (Streamlit Dashboard) |
+-----------------+                                  +----------------+
```

### 2. Prerequisites (Before You Start)

1.  **AWS Account Access:** You need an AWS account with administrative privileges (or a role that allows you to create IAM roles, S3 buckets, Lambda functions, and App Runner services).
2.  **AWS CLI Configured:** Make sure your AWS Command Line Interface (CLI) is installed and configured with your AWS credentials (access key, secret key, default region). You can test this by running `aws sts get-caller-identity`.
3.  **Git Repository:** Your project code should be in a Git repository (e.g., GitHub, AWS CodeCommit). App Runner can pull directly from GitHub.
4.  **Local Python Environment:** Ensure your local machine has Python installed and you've run `pip install -r requirements.txt` so you can test locally.
5.  **Snowflake Credentials:** Have your Snowflake `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_ROLE`, and `SNOWFLAKE_WAREHOUSE` readily available.
6.  **Understanding of Python Packaging:** We'll need to package Python dependencies for Lambda.

### 3. Step-by-Step Deployment

#### Step 3.1: Prepare Your Codebase for AWS

Our current code assumes local file paths (`.env`, `dq_reports/dq_results.json`). We need to adapt it for cloud execution.

1.  **Remove `.env` from Project Root:** Ensure your `.env` file is *not* committed to Git. It contains sensitive data.
2.  **Modify `dq_dashboard.py` to read from S3:**
    *   Change `RESULTS_FILE` from `REPORTS_DIR / "dq_results.json"` to read from an S3 bucket.
    *   Add `boto3` (AWS SDK for Python) to `requirements.txt`.
    *   **Junior's Task:**
        *   Open `dq_dashboard.py`.
        *   Modify the `RESULTS_FILE` constant:
            ```python
            # dq_dashboard.py
            # ...
            # REPORTS_DIR = PROJECT_ROOT / "dq_reports" # REMOVE OR COMMENT OUT THIS LINE
            # RESULTS_FILE = REPORTS_DIR / "dq_results.json" # REMOVE OR COMMENT OUT THIS LINE

            import boto3 # Add this import
            import io # Add this import

            # New S3 Configuration
            S3_BUCKET_NAME = os.getenv("S3_DQ_REPORTS_BUCKET") # Get bucket name from env var
            S3_RESULTS_KEY = "dq_results.json" # The name of the file in S3

            # Modify load_results function
            @st.cache_data(ttl=60)
            def load_results():
                """Loads the latest DQ results from S3."""
                if not S3_BUCKET_NAME:
                    st.error("Error: S3_DQ_REPORTS_BUCKET environment variable not set.")
                    return pd.DataFrame()
                
                s3 = boto3.client('s3')
                try:
                    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=S3_RESULTS_KEY)
                    data = json.loads(obj['Body'].read())
                    df = pd.DataFrame(data)
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    return df
                except s3.exceptions.NoSuchKey:
                    st.warning(f"S3 object '{S3_RESULTS_KEY}' not found in bucket '{S3_BUCKET_NAME}'. No DQ results yet.")
                    return pd.DataFrame()
                except (json.JSONDecodeError, pd.errors.EmptyDataError) as e:
                    st.error(f"Error loading DQ results from S3: {e}. The JSON file might be empty or malformed.")
                    return pd.DataFrame()
                except Exception as e:
                    st.error(f"An unexpected error occurred while loading S3 data: {e}")
                    return pd.DataFrame()
            ```
        *   Add `boto3` to `requirements.txt`:
            ```
            # requirements.txt
            snowflake-connector-python[pandas]
            python-dotenv
            pandas
            streamlit
            pyyaml
            boto3 # ADD THIS LINE
            ```

3.  **Modify `run_automatic_dq_checks.py` to write to S3:**
    *   Change `RESULTS_FILE` path to write directly to S3.
    *   **Junior's Task:**
        *   Open `run_automatic_dq_checks.py`.
        *   Add `boto3` and `io` imports:
            ```python
            # run_automatic_dq_checks.py
            # ...
            import boto3 # Add this import
            import io # Add this import

            # Define output directory and file for DQ reports
            # OUTPUT_DIR = project_root / "dq_reports" # REMOVE OR COMMENT OUT
            # OUTPUT_DIR.mkdir(exist_ok=True) # REMOVE OR COMMENT OUT
            # RESULTS_FILE = OUTPUT_DIR / "dq_results.json" # REMOVE OR COMMENT OUT

            # New S3 Configuration
            S3_BUCKET_NAME = os.getenv("S3_DQ_REPORTS_BUCKET") # Get bucket name from env var
            S3_RESULTS_KEY = "dq_results.json" # The name of the file in S3

            # Modify the main function's results saving part
            def main():
                # ... existing code ...
                if results:
                    s3 = boto3.client('s3')
                    json_data = json.dumps(results, indent=2)
                    try:
                        s3.put_object(Bucket=S3_BUCKET_NAME, Key=S3_RESULTS_KEY, Body=json_data)
                        logging.info(f"✅ Data quality checks complete. Results saved to s3://{S3_BUCKET_NAME}/{S3_RESULTS_KEY}")
                    except Exception as e:
                        logging.error(f"❌ Failed to save results to S3: {e}")
                        # Optionally, save to local file as fallback if S3 fails (for debugging)
                        # with open(project_root / "dq_reports" / "dq_results_fallback.json", 'w') as f:
                        #     f.write(json_data)
                        # logging.info("✅ Also saved a fallback local copy.")
                else:
                    logging.warning("No results were generated.")
                    # Optionally, create an empty S3 object if no results
                    # s3 = boto3.client('s3')
                    # s3.put_object(Bucket=S3_BUCKET_NAME, Key=S3_RESULTS_KEY, Body=json.dumps([]))
                # ... rest of the main function ...
            ```
        *   `boto3` is already in `requirements.txt` from the dashboard step.

#### Step 3.2: Set Up AWS Resources

1.  **Create an S3 Bucket for DQ Reports:**
    *   This bucket will store your `dq_results.json`.
    *   Choose a globally unique name (e.g., `your-company-dq-reports-2023`).
    *   **Junior's Task:**
        ```bash
        aws s3 mb s3://your-company-dq-reports-2023 --region us-east-1 # Use your preferred region
        ```
        *   **Why:** S3 is highly durable and cost-effective for storing semi-structured data like JSON reports.

2.  **Store Snowflake Credentials in AWS Secrets Manager:**
    *   This is the most secure way to handle credentials.
    *   **Junior's Task:**
        *   Go to AWS Console -> Secrets Manager.
        *   Click "Store a new secret".
        *   Choose "Other type of secret".
        *   Use "Plaintext" (for now, for simplicity, though JSON is also an option).
        *   Paste your `.env` content *exactly as is*, but without the `SNOWFLAKE_` prefix for the keys (Secrets Manager will automatically put them as key-value pairs if you choose JSON, but for plaintext, it's just the value).
            *   **Correction for Junior:** Secrets Manager *prefers* JSON structure for key-value pairs. Let's make it a JSON secret.
            *   Choose "Credentials for other database".
            *   Set "Secret key/value" using `key=value` for each Snowflake credential.
            *   **Better Junior's Task for Secrets Manager:**
                *   Secret Type: `Other type of secret`
                *   Key/Value pairs:
                    *   `user`: `your_snowflake_user`
                    *   `password`: `your_snowflake_password`
                    *   `account`: `your_snowflake_account_identifier`
                    *   `role`: `your_snowflake_role`
                    *   `warehouse`: `your_snowflake_warehouse`
                *   Name your secret: `snowflake/dq-platform-credentials`
                *   Click "Next" until "Store".
        *   **Why:** Secrets Manager integrates directly with IAM, allowing you to grant specific services (like Lambda and App Runner) permission to *read* the secret, without exposing the secret itself.

3.  **Create IAM Role for Lambda (DQ Runner):**
    *   This role will grant Lambda permissions to access Secrets Manager and S3.
    *   **Junior's Task:**
        *   Go to AWS Console -> IAM -> Roles -> "Create role".
        *   Select "AWS service" -> "Lambda".
        *   **Permissions:**
            *   Attach `AWSLambdaBasicExecutionRole` (for CloudWatch Logs).
            *   Attach `SecretsManagerReadWrite` (or a more specific policy: `secretsmanager:GetSecretValue` to the secret `arn:aws:secretsmanager:REGION:ACCOUNT_ID:secret:snowflake/dq-platform-credentials-*`).
            *   Attach `AmazonS3FullAccess` (or a more specific policy: `s3:PutObject` on `arn:aws:s3:::your-company-dq-reports-2023/*`).
        *   Name the role: `LambdaDQPlatformRole`
        *   **Why:** Least privilege. Lambda needs only the permissions necessary to perform its job.

4.  **Create IAM Role for App Runner (Dashboard):**
    *   This role will grant App Runner permissions to read from S3 (the DQ report) and Secrets Manager (Snowflake credentials if the dashboard also needed them directly, but in our design, it just needs the S3 bucket name).
    *   **Junior's Task:**
        *   Go to AWS Console -> IAM -> Roles -> "Create role".
        *   Select "AWS service" -> "App Runner".
        *   **Permissions:**
            *   Attach `AmazonS3ReadOnlyAccess` (or more specific: `s3:GetObject` on `arn:aws:s3:::your-company-dq-reports-2023/*`).
            *   Attach `SecretsManagerReadWrite` (or `secretsmanager:GetSecretValue` on your secret ARN).
        *   Name the role: `AppRunnerDQPlatformRole`
        *   **Why:** Same as Lambda, granting minimal necessary permissions.

#### Step 3.3: Deploy the DQ Check Runner (Lambda)

1.  **Package Your Python Code and Dependencies:**
    *   Lambda requires your code and all its dependencies (from `requirements.txt`) to be bundled into a `.zip` file.
    *   **Junior's Task:**
        *   Navigate to your project root in the terminal (where `requirements.txt` and your `.py` files are).
        *   Create a directory for dependencies: `mkdir package`
        *   Install dependencies into this directory: `pip install -r requirements.txt -t package/`
        *   Move your application files into the `package` directory:
            ```bash
            cp *.py automatic_dq_rules.py config.py utils.py package/
            # If you had dq_rules/ directory and wanted to include it for future use:
            # cp -r dq_rules/ package/dq_rules/
            ```
        *   Navigate *into* the `package` directory.
        *   Create the zip file: `zip -r ../dq_lambda_package.zip .`
        *   Now you have `dq_lambda_package.zip` one level up from `package/`.
        *   **Why:** Lambda executes code from a deployment package. All external libraries must be included.

2.  **Create the Lambda Function:**
    *   **Junior's Task:**
        *   Go to AWS Console -> Lambda -> "Create function".
        *   Author from scratch.
        *   Function name: `dq-platform-runner`
        *   Runtime: `Python 3.9` (or latest available supported by your requirements).
        *   Architecture: `x86_64`
        *   Execution role: Choose "Use an existing role" and select `LambdaDQPlatformRole`.
        *   Click "Create function".
        *   **Upload the code:** In the "Code" tab, click "Upload from" -> ".zip file". Upload `dq_lambda_package.zip`.
        *   **Handler:** Set handler to `run_automatic_dq_checks.main` (the filename without `.py` then `.main` function).
        *   **Environment Variables:**
            *   `S3_DQ_REPORTS_BUCKET`: `your-company-dq-reports-2023` (your S3 bucket name)
            *   `SNOWFLAKE_SECRET_NAME`: `snowflake/dq-platform-credentials` (the name of your secret in Secrets Manager)
        *   **Why:** Lambda needs to know where its code is and how to get its runtime configuration.

3.  **Configure Lambda to get Secrets from Secrets Manager:**
    *   Your `utils.py` currently uses `os.getenv` for Snowflake credentials. We need to modify `get_snowflake_connection` to fetch from Secrets Manager using `boto3`.
    *   **Junior's Task:**
        *   Open `utils.py`.
        *   Modify `get_snowflake_connection` function:
            ```python
            # utils.py
            # ...
            import boto3 # Add this import
            # ...

            # Remove load_dotenv(dotenv_path) as it's not needed in Lambda
            # load_dotenv() # REMOVE OR COMMENT OUT THIS LINE

            def get_snowflake_connection() -> Optional[snowflake.connector.SnowflakeConnection]:
                """
                Establishes a connection to Snowflake using credentials from AWS Secrets Manager.
                """
                secret_name = os.getenv("SNOWFLAKE_SECRET_NAME")
                if not secret_name:
                    logger.critical("SNOWFLAKE_SECRET_NAME environment variable not set.")
                    return None

                # Fetch secrets from Secrets Manager
                session = boto3.session.Session()
                client = session.client(
                    service_name='secretsmanager',
                    region_name=os.getenv("AWS_REGION", "us-east-1") # Ensure AWS_REGION is set in Lambda env or default
                )
                
                try:
                    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
                    if 'SecretString' in get_secret_value_response:
                        secret = json.loads(get_secret_value_response['SecretString'])
                    else:
                        logger.critical("Secret value not found or not in SecretString format.")
                        return None
                except Exception as e:
                    logger.critical(f"Could not retrieve secret '{secret_name}' from Secrets Manager. Error: {e}")
                    return None

                try:
                    conn = snowflake.connector.connect(
                        user=secret["user"],
                        password=secret["password"],
                        account=secret["account"],
                        role=secret["role"],
                        warehouse=secret["warehouse"]
                    )
                    logger.info("Successfully connected to Snowflake.")
                    return conn
                except Exception as e:
                    logger.critical(f"Could not connect to Snowflake. Aborting. Error: {e}")
                    return None
            ```
        *   **Re-package Lambda:** After modifying `utils.py`, you *must* re-do Step 3.3.1 (packaging) and re-upload the `.zip` file to Lambda.

4.  **Set up EventBridge Schedule:**
    *   To make your DQ checks run automatically.
    *   **Junior's Task:**
        *   Go to AWS Console -> EventBridge -> Rules -> "Create rule".
        *   Name: `dq-platform-daily-schedule`
        *   Rule type: "Schedule"
        *   Define a schedule: `Cron expression` (e.g., `cron(0 6 * * ? *)` for 6 AM UTC daily) or `Rate expression`.
        *   Targets: Select "Lambda function" and choose `dq-platform-runner`.
        *   Click "Create".
        *   **Why:** EventBridge provides robust, scalable scheduling capabilities.

#### Step 3.4: Deploy the DQ Dashboard (App Runner)

1.  **Push Your Code to a Git Repository:**
    *   Ensure your `dq_dashboard.py` (with S3 reading logic) and `requirements.txt` (with `boto3`) are pushed to GitHub or CodeCommit.

2.  **Create the App Runner Service:**
    *   **Junior's Task:**
        *   Go to AWS Console -> App Runner -> "Create service".
        *   **Source:**
            *   Source: "Source code repository"
            *   Repository type: "GitHub" (or your chosen Git provider)
            *   Connect: Follow instructions to connect your GitHub account.
            *   Repository: Select your project repository.
            *   Branch: `main` (or your default branch).
            *   Deployment trigger: "Automatic" (recommended for continuous deployment).
        *   **Build & Deployment:**
            *   Runtime: `Python 3`
            *   Build command: `pip install -r requirements.txt` (App Runner builds the image for you).
            *   Start command: `streamlit run dq_dashboard.py --server.port 8080 --server.address 0.0.0.0`
                *   **Why `--server.port 8080 --server.address 0.0.0.0`:** App Runner requires your application to listen on port 8080 and bind to `0.0.0.0`. Streamlit defaults to 8501 and localhost.
            *   Port: `8080`
        *   **Service Settings:**
            *   Service name: `dq-platform-dashboard`
            *   CPU/Memory: Start with `1 vCPU, 2 GB` for testing, can scale up/down later.
        *   **Environment Variables:**
            *   `S3_DQ_REPORTS_BUCKET`: `your-company-dq-reports-2023` (your S3 bucket name)
            *   `SNOWFLAKE_SECRET_NAME`: `snowflake/dq-platform-credentials` (name of your secret, if dashboard also needed direct Snowflake access)
        *   **Security:**
            *   Instance role: Select `AppRunnerDQPlatformRole`. This grants the running application permissions.
        *   Review and "Create & deploy".
        *   **Why:** App Runner simplifies web app deployment, handles infrastructure, scaling, and networking for you, making it ideal for Streamlit.

### 4. Post-Deployment Verification

1.  **Check Lambda Execution:**
    *   Go to AWS Console -> Lambda -> `dq-platform-runner`.
    *   In the "Monitor" tab, check CloudWatch Logs for recent invocations. Look for `INFO` messages like "Successfully connected to Snowflake." and "Results saved to s3://..."
    *   You can also manually trigger the Lambda function for immediate testing by clicking "Test" (create a simple test event with `{}`).
2.  **Verify S3 Report:**
    *   Go to AWS Console -> S3 -> your `your-company-dq-reports-2023` bucket.
    *   You should see `dq_results.json` object. Download it and check its contents to ensure it's valid JSON.
3.  **Access App Runner Dashboard:**
    *   Go to AWS Console -> App Runner -> `dq-platform-dashboard`.
    *   Once the service status is "Running", click on the "Default domain" URL.
    *   Your Streamlit dashboard should load and display the data quality results from S3.
    *   **Troubleshooting Tip:** If the dashboard doesn't load or shows errors, check the "Logs" tab in the App Runner service console for Python or Streamlit errors.

### 5. Maintenance & Monitoring

*   **CloudWatch Logs:** Regularly check the logs for your Lambda function and App Runner service for errors or warnings.
*   **CloudWatch Alarms:** Set up alarms for Lambda errors, App Runner unhealthy hosts, or specific log patterns indicating DQ failures.
*   **Budgets:** Create AWS Budgets to monitor costs associated with Lambda, S3, and App Runner.
*   **Updates:** When you update your Python code:
    *   **For Lambda:** Re-package the `.zip` file (Step 3.3.1) and re-upload it to the Lambda function.
    *   **For App Runner:** If you set "Deployment trigger" to "Automatic", pushing changes to your Git branch will automatically trigger a new deployment. Otherwise, manually deploy from the App Runner console.
*   **Secrets Rotation:** Configure automatic rotation for your Snowflake credentials in Secrets Manager for enhanced security.

### 6. Advanced Considerations (For Future)

*   **VPC Configuration:** For enhanced security and if your Snowflake instance or other AWS resources are in a private network, configure Lambda and App Runner to operate within a VPC. This adds complexity with private subnets, NAT Gateways, and VPC Endpoints.
*   **CI/CD Pipeline:** Automate the deployment process using AWS CodePipeline, CodeBuild, and CodeCommit (or GitHub Actions). This ensures consistent and reliable deployments on every code change.
*   **Lambda Layers:** For common dependencies like `pandas` and `snowflake-connector-python`, create a Lambda Layer. This makes your deployment package smaller and faster to upload, especially if you have multiple Lambda functions using the same libraries.
*   **Terraform/CloudFormation:** For Infrastructure as Code, use tools like Terraform or AWS CloudFormation to define and manage your AWS resources. This ensures your infrastructure is version-controlled and reproducible.
*   **Cost Optimization:** Monitor usage patterns and adjust Lambda memory/timeout, App Runner CPU/Memory, or S3 storage tiers as needed to optimize costs.
