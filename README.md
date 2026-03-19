Yelp Recommender System

Course
Distributed Data Systems

Project Summary

This project builds an end to end recommendation system using Yelp review data. The system combines cloud storage, workflow orchestration, NoSQL storage, machine learning, API serving, containerization, and cloud deployment.

The pipeline begins with Yelp review data stored in Google Cloud Storage. Apache Airflow orchestrates the workflow and loads the first 5000 Yelp reviews into MongoDB. MongoDB stores the raw reviews as well as processed analytics collections.

The pipeline then exports a training dataset and trains a Funk SVD recommendation model. After training, the system generates top recommendations for users and stores them in MongoDB.

A FastAPI service reads the stored recommendations and exposes them through a REST API endpoint. The API is containerized using Docker and deployed on Google Cloud Run so it can be accessed publicly.

To simplify development and testing, Docker Compose is used to start MongoDB and the API service locally.

Architecture

Yelp Dataset in Google Cloud Storage
to Airflow Pipeline
to MongoDB raw_reviews collection
to MongoDB business_star_stats collection
to MongoDB yearly_star_stats collection
to training data export
to Funk SVD model training
to recommendations collection
to FastAPI recommendation service
to Docker container
to Google Cloud Run deployment

Main Components
	1.	Google Cloud Storage
Stores the Yelp dataset used for the pipeline.
	2.	Apache Airflow
Orchestrates the full data pipeline and ensures each task runs in the correct order.
	3.	MongoDB
Stores the raw review data, aggregated statistics, and generated recommendations.
	4.	Funk SVD Model
Collaborative filtering model that learns latent user and business factors from rating data.
	5.	FastAPI
Provides a REST API that returns recommendations for a user.
	6.	Docker and Docker Compose
Docker packages the API service into a container.
Docker Compose simplifies running MongoDB and the API locally.
	7.	Google Cloud Run
Hosts the API container as a serverless cloud service.

MongoDB Collections

raw_reviews
Stores the Yelp review documents loaded from the dataset.

business_star_stats
Stores aggregated statistics per business including average rating and review count.

yearly_star_stats
Stores aggregated statistics per business and year.

recommendations
Stores top recommended businesses for each user.

Airflow Pipeline Tasks
	1.	load_reviews_to_mongo
Loads Yelp review data into MongoDB.
	2.	create_business_star_stats
Creates aggregated statistics grouped by business.
	3.	create_yearly_star_stats
Creates yearly rating statistics grouped by business and year.
	4.	export_training_data
Exports user business rating data used for training the recommendation model.
	5.	train_recommender_model
Trains the Funk SVD collaborative filtering model.
	6.	generate_and_store_recommendations
Generates top recommendations and stores them in MongoDB.

Project Files

yelp_pipeline_dag.py
Airflow DAG that orchestrates the full data pipeline.

src/train_funk_svd.py
Script that trains the Funk SVD recommendation model.

src/generate_recommendations.py
Script that generates and stores recommendations in MongoDB.

src/api.py
FastAPI service that returns recommendations through an API endpoint.

requirements.txt
Python dependencies used for the full project environment.

Dockerfile
Docker container definition used to build the API service.

docker-compose.yml
Starts MongoDB and the API service locally for development.

How to Run the Pipeline
	1.	Start MongoDB or run Docker Compose.

docker compose up
	2.	Start Apache Airflow.
	3.	Place yelp_pipeline_dag.py in the Airflow dags folder.
	4.	Trigger the DAG in the Airflow interface.
	5.	Wait for all tasks to complete successfully.
	6.	Verify the MongoDB collections:

raw_reviews
business_star_stats
yearly_star_stats
recommendations

Running the API Locally

Start MongoDB and the API service:

docker compose up –build

Then open:

http://localhost:8000

Interactive API documentation:

http://localhost:8000/docs

To retrieve recommendations for a user:

http://localhost:8000/recommendations/<user_id>

Cloud Run Deployment

The FastAPI service was containerized with Docker and deployed to Google Cloud Run.

Deployed service URL

https://yelp-api-695117127505.us-west1.run.app

Cloud Run allows the API to run in the cloud and automatically scales when requests are made.

Notes

The pipeline currently loads the first 5000 Yelp reviews to keep processing time short during development and demonstration.

Recommendations are precomputed and stored in MongoDB. This allows the API to return results quickly without recomputing predictions for each request.

The system demonstrates a distributed data engineering workflow that includes cloud storage, pipeline orchestration, NoSQL databases, recommendation modeling, API serving, containerized services, and cloud deployment.