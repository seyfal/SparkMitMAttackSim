
# Spark MITM Attack Simulation

## Table of Contents
1. [Introduction](#introduction)
2. [Quick Start](#quick-start)
3. [Video Walkthru](#video-walkthru)
4. [System Architecture](#system-architecture)
5. [Code Logic and Flow](#code-logic-and-flow)
   - [Initialization](#initialization)
   - [Data Loading and Pre-processing](#data-loading-and-pre-processing)
   - [Distributed Matching](#distributed-matching)
   - [Post-processing](#post-processing)
   - [Result Compilation](#result-compilation)
6. [Generated Statistics](#generated-statistics)
   - [Overview](#overview)
   - [Matching Metrics](#matching-metrics)
   - [Performance Metrics](#performance-metrics)
   - [Error Metrics](#error-metrics)
7. [Areas of Improvement](#areas-of-improvement)
8. [Known Bugs and Issues](#known-bugs-and-issues)
9. [References and Citations](#references-and-citations)

---

## Introduction

This project is designed to simulate the behavior of Man-in-the-Middle (MitM) attackers within large graph structures through parallel random walks. Our focus is to understand the efficacy of such attacks, particularly when attackers have prior knowledge of the graph's topology but cannot discern between authentic nodes and honeypots. The core of this project lies in its ability to represent network communication pathways and analyze the movement of potential threats within them.

The simulation involves performing random walks on perturbed graphs, identifying nodes with valuable data, and applying the SimRank algorithm to differentiate between genuine nodes and honeypots. The primary aim is to generate attacker statistics, providing insights into the success and failure rates of simulated attacks under varying conditions.

---

## Quick Start

### Prerequisites
- Apache Spark 3.x
- Scala 2.12.x
- sbt 1.x

### Setup and Execution
1. Clone the repository:
   ```shell
   git clone [repository_url]
   ```
2. Navigate to the project directory:
   ```shell
   cd SparkRandomWalk
   ```
3. Compile the project:
   ```shell
   sbt compile
   ```
4. Run the simulations:
   ```shell
   sbt run
   ```

## Video Walkthru

_A video walkthru is not yet available for this project. This section will be updated once the video content has been created._

---

## System Architecture

The system architecture is designed to leverage distributed computing for processing large-scale graphs efficiently. It employs Apache Spark's GraphX library to handle graph operations in parallel, ensuring scalability and fault tolerance.

![System Architecture Diagram](#) _[Image placeholder for system architecture visual representation]_

Key Components:
- **Spark Context**: Initializes the main entry point for Spark functionality and sets up internal services.
- **GraphX API**: Utilizes Spark's API for graphs and graph-parallel computation.
- **SimRank Algorithm**: A pivotal algorithm for assessing the similarity of nodes within a graph.

---

## Code Logic and Flow

### Initialization
The initialization process involves setting up the Spark context and defining the configuration parameters, such as the number of iterations for random walks and thresholds for the SimRank algorithm.

### Data Loading and Pre-processing
During this phase, the program reads the original and perturbed graph data, parsing nodes and edges to prepare them for processing. This step ensures that the data is in the correct format for the GraphX operations.

### Distributed Matching
For each random walk executed, the program computes the likelihood of nodes in the perturbed graph matching with nodes in the original graph. This computation is distributed across the cluster for parallel processing.

### Post-processing
After the distributed matching, the system aggregates the results and applies the decision logic to determine whether an attack was successful or a honeypot was encountered.

### Result Compilation
The final step compiles the results of the simulation, including the number of successful and failed attacks, and computes the statistics related to the random walks.

---

## Generated Statistics

### Overview
The generated statistics provide a comprehensive look into the effectiveness of the simulated MitM attacks. They help in understanding the behavior of the algorithm under various conditions.

### Matching Metrics
- **True Positives**: Number of correctly identified valuable nodes.
- **False Positives**: Number of honeypots incorrectly identified as valuable nodes.

### Performance Metrics
- **Success Rate**: The ratio of successful attacks to the total number of attacks.
- **Walk Statistics**: Min, max, median, and mean number of nodes visited in the random walks.

### Error Metrics
Error metrics provide insight into the accuracy of the SimRank algorithm in differentiating between genuine nodes and honeypots.

---

## Areas of Improvement

The following areas have been identified for potential improvement:
- **Algorithm Optimization**: Enhancing the SimRank algorithm's performance for large-scale graphs.
- **Scalability**: Further testing and optimization for extremely large graph datasets.
- **Usability**: Development of a user-friendly interface for monitoring and controlling simulation parameters.

---

## Known Bugs and Issues

No known bugs have been reported as of the latest release. Users are encouraged to report any issues they encounter to help improve the project.

---

## References and Citations

