# Pinterest Data Pipeline

## Table of Contents

1. [Project Description](#project-description)
1. [Installation](#installation)
1. [Usage](#usage)
1. [File Structure](#file-structure)
1. [License](#license)

## Project Description

This project aims to process data streams from a Pinterest API emulator.

Pinterest data is produced locally then sent to kafka topics running on an EC2 instance were they are stored in an S3 bucket.

## Installation

### Required Credentails

1. The AWS RDS credentails for the Pinterest API emulator store in a yaml file (#db_creds.yaml) with the form:

```yaml
HOST:
  <insert host adresse>
USER:
  <insert user name>
PASSWORD:
  <insert RDS password>
DATABASE:
  <insert database name>
PORT:
  <insert port>
```

1. An EC2 private key store in a pem file (Key pair name.pem).

## Usage

```mermaid
flowchart TB
  
  subgraph a[Pinterest Data Pipeline]
      direction LR
        
        subgraph i[Databricks]
          direction LR
            j[Mount S3 Bucket] --> k[Save Each Topic a Dataframe];
        end

        subgraph f[EC2 instance]
          direction LR
            g[Save Data to 1 of 3 Topics <br> EC2 Kafka Client] --> h[Send Data to S3 Bucket <br> MSK Connect];
        end

        subgraph b[user_posting_emulation.py]
          direction LR
            c[Get Data Entries  <br>  RDS Connector] --> d[Save Entries as <br> Dictionaries];
            d --> e[Send Entries to EC2 <br> REST API]
        end

        
    end
```

## File Structure

### user_posting_emulation.py

This python file has one Class AWSDBConnector whose attributes are the credentails (HOST, USER, PASSWORD, DATABASE, PORT) used to connect to an RDS of Pinterest data. This Class has three methods:

- read_db_creds - This takes in a yaml file with the nessecary credentails and returns a dictionary of these credentials
- post_to_API - This take some Pinterest data and sends it to an API
- create_db_connector - Creates the RDS connection 

## License

MIT License

Copyright (c) [2024] [Chile Mwamba]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.