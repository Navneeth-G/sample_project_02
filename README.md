#  Building a Resilient ELT Pipeline: From Kubernetes Pods to AWS S3 to Snowflake

##  Introduction: The Significance of This Project

In many enterprise environments, critical operational data is generated and stored within ephemeral systems, such as Kubernetes pods. These systems often produce configuration and usage files that are vital for auditing, resource allocation, and compliance. However, these files are transient by nature—overwritten or deleted daily—and are not readily accessible from outside the cluster due to security and architectural constraints.

Our challenge was to design an ELT (Extract, Load, Transform) pipeline capable of:

* **Extracting** ephemeral data from inaccessible Kubernetes pods.
* **Loading** the data into a centralized, durable storage system (AWS S3).
* **Transforming** and ingesting the data into a structured data warehouse (Snowflake) for analysis and reporting.

This project underscores the importance of building flexible, modular, and resilient data pipelines that can adapt to changing environments and requirements.

> **Key Takeaways:**
>
> * Critical data often resides in transient and inaccessible systems.
> * Designing pipelines for such data requires innovative extraction and loading strategies.
> * Flexibility and modularity are essential for adapting to evolving infrastructure and requirements.

---

##  Understanding the Challenge: Data Location and Accessibility

The data in question resided within text-based configuration files inside Kubernetes pods. These files were:

* **Ephemeral**: Overwritten or deleted daily, making timely extraction crucial.
* **Inaccessible**: Stored within pods that could not be directly accessed or mounted from outside the cluster.
* **Non-standard**: Formatted in a human-readable but non-standard structure, complicating automated parsing.

An example of the file structure:

```
Begin UserGroup
GROUP_NAME    GROUP_MEMBER         USER_SHARES
team1         (user1 user2)        ([user1, 3] [user2, 2])
End UserGroup
```

Given that our orchestration tool, Airflow, operated in a separate Kubernetes cluster with no direct access to these pods, we needed a method to execute data extraction within the pods themselves and then transfer the results to a location accessible by Airflow.

> **Key Takeaways:**
>
> * Data was transient and required timely extraction.
> * Direct access to the data was not possible due to cluster isolation.
> * Custom parsing was necessary due to non-standard file formats.

---

##  Architectural Decisions: Structuring the Pipeline

To address these challenges, we adopted a modular architecture comprising three main components:

### 1. Configuration Files

We centralized all environment-specific details—such as pod identifiers, file paths, S3 bucket names, and Snowflake table names—into a single JSON configuration file. This approach allowed us to manage changes in the environment without altering the core logic of the pipeline.

### 2. Configuration Handler

A Python module was developed to read the JSON configuration and output a flat dictionary of parameters. This abstraction enabled the main logic scripts to remain agnostic of the configuration source, facilitating future transitions to environment variables or Airflow variables if needed.

### 3. Main Logic Scripts

Two primary scripts were created:

* **Record Counting Script**: Executed within the Kubernetes pod to count the number of records in the source file, providing a baseline for validation.
* **Parsing and Uploading Script**: Also executed within the pod, this script parsed the source file, transformed the data into NDJSON format, and uploaded it to AWS S3.

This separation of concerns ensured that each component could be developed, tested, and maintained independently, enhancing the pipeline's robustness and adaptability.

> **Key Takeaways:**
>
> * Modular architecture facilitates maintenance and scalability.
> * Separation of configuration, handling, and logic promotes code clarity and reusability.
> * Executing scripts within the data's native environment overcomes accessibility constraints.

---

##  Deployment Constraints: Navigating Infrastructure Limitations

Operating within isolated Kubernetes clusters presented several deployment challenges:

* **Airflow's Limitations**: Running in a separate cluster, Airflow could not directly access the data pods or their file systems.
* **Security Restrictions**: Inter-cluster communication was restricted, preventing direct data transfer between pods.
* **Data Volume Considerations**: Transferring large volumes of data through Airflow's XCom mechanism was impractical.

To circumvent these issues, we implemented the following strategy:

1. **Script Deployment**: Airflow uploaded the necessary scripts to the target Kubernetes pods using secure channels.
2. **In-Pod Execution**: The scripts were executed within the pods, accessing and processing the data locally.
3. **Data Transfer**: Processed data was uploaded from the pods to AWS S3, a location accessible by Airflow for further processing.

This approach ensured that data extraction and transformation occurred within the secure confines of the data pods, while Airflow managed orchestration and downstream processing.

> **Key Takeaways:**
>
> * Infrastructure constraints necessitated in-pod data processing.
> * Secure script deployment and execution within pods enabled data extraction.
> * AWS S3 served as an intermediary storage accessible by both the pods and Airflow.

---

## 🧾 Data Parsing: Understanding the Source Data

The source files contained multiple data blocks, each encapsulated between `Begin` and `End` markers. Our focus was on the `UserGroup` blocks, which detailed user group configurations and resource allocations.

Example:

```
Begin UserGroup
GROUP_NAME    GROUP_MEMBER         USER_SHARES
team1         (user1 user2)        ([user1, 3] [user2, 2])
End UserGroup
```

Our parsing logic extracted the following fields:

* **Group Name**: Identifier of the user group.
* **User Names**: List of users within the group.
* **User Shares**: Resource allocation for each user.

The extracted data was transformed into NDJSON format, with each line representing a single record:

```json
{"group": "team1", "user": "user1", "share": 3, "timestamp": "2025-06-06T10:00:00Z"}
{"group": "team1", "user": "user2", "share": 2, "timestamp": "2025-06-06T10:00:00Z"}
```

This format facilitated efficient storage and processing in downstream systems.

> **Key Takeaways:**
>
> * Focused parsing on relevant data blocks ensured efficiency.
> * NDJSON format enabled scalable storage and processing.
> * Timestamping records provided temporal context for analysis.

---

##  Data Flow: From Extraction to Ingestion

The end-to-end data flow encompassed several stages:

1. **Record Counting**: Executed within the pod to determine the number of records, aiding in validation and resource planning.
2. **Data Parsing and Uploading**: Processed data was transformed into NDJSON format and uploaded to AWS S3.
3. **Data Ingestion**: Airflow triggered Snowflake to ingest the data from S3 into a raw table.
4. **Validation**: Record counts from the source and Snowflake were compared to ensure data integrity.
5. **Audit Logging**: Results and statuses were logged in an audit table within Snowflake for monitoring and troubleshooting.

This structured flow ensured data integrity, traceability, and facilitated error detection and recovery.

> **Key Takeaways:**
>
> * Structured data flow enabled systematic processing and validation.
> * Audit logging provided transparency and facilitated issue resolution.
> * Modular stages allowed for targeted troubleshooting and maintenance.

---

##  Error Handling and Recovery: Ensuring Resilience

Recognizing the potential for failures at various stages, we implemented robust error handling and recovery mechanisms:

* **Retries**: Failed stages were configured to retry a predefined number of times with exponential backoff.
* **Alerting**: Failures triggered alerts to the engineering team for immediate investigation.
* **Cleanup**: Partial or corrupted data in S3 or Snowflake was automatically cleaned up to prevent inconsistencies.
* **Audit Trails**: Detailed logs and audit records were maintained to facilitate root cause analysis.

These measures ensured that transient issues could be resolved automatically, while persistent problems were promptly addressed by the engineering team.

> **Key Takeaways:**
>
> * Automated retries mitigated transient failures.
> * Alerting and logging facilitated rapid issue detection and resolution.
> * Cleanup procedures maintained data integrity across systems.

---

##  Audit Table: Tracking Pipeline Execution

An audit table in Snowflake was established to monitor pipeline executions. Key fields included:

* **Execution Date**: Date of the pipeline run.
* **Source Record Count**: Number of records extracted from the source file.
* **Ingested Record Count**: Number of records successfully ingested into Snowflake.
* **Status**: Execution status (e.g., Success, Failed).
* **Error Messages**: Details of any errors encountered.

This table served as a central repository for monitoring pipeline health, facilitating reporting, and supporting compliance requirements.

> **Key Takeaways:**
>
> * Centralized audit logging enabled comprehensive monitoring.
> * Detailed records supported compliance and reporting needs.
> * Historical data facilitated trend analysis and capacity planning.

---

##  Glossary: Key Terms and Concepts

* **Kubernetes Pod**: The smallest deployable unit in Kubernetes, encapsulating one or more containers.
* **Airflow**: An open-source platform to programmatically author, schedule, and monitor workflows.
* **NDJSON (Newline Delimited JSON)**: A format where each line is a valid JSON object, facilitating streaming and incremental processing.
* **AWS S3 (Simple Storage Service)**: A scalable object storage service provided by Amazon Web Services.
* **Snowflake**: A cloud-based data warehousing platform that supports scalable data storage and processing.

> **Key Takeaways:**
>
> * Understanding key terms is essential for grasping pipeline components and operations.
> * Familiarity with tools and formats facilitates effective development and troubleshooting.

---

This documentation serves as both a technical manual and a learning resource, empowering junior data engineers to understand the intricacies of building resilient and adaptable data pipelines.
