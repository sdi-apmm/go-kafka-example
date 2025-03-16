package model

import "time"

// GitHubEvent represents the common fields in all GitHub webhook events
type GitHubEvent struct {
	Action      string      `json:"action,omitempty"`
	Repository  Repository  `json:"repository"`
	Installation Installation `json:"installation,omitempty"`
}

// Repository represents a GitHub repository
type Repository struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	FullName string `json:"full_name"`
	HTMLURL  string `json:"html_url"`
}

// Installation represents a GitHub app installation
type Installation struct {
	ID int64 `json:"id"`
}

// DeploymentEvent represents a GitHub deployment event
type DeploymentEvent struct {
	GitHubEvent
	Deployment    Deployment    `json:"deployment"`
	DeploymentStatus DeploymentStatus `json:"deployment_status,omitempty"`
}

// Deployment represents a GitHub deployment
type Deployment struct {
	ID          int64     `json:"id"`
	Ref         string    `json:"ref"`
	Task        string    `json:"task"`
	Environment string    `json:"environment"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// DeploymentStatus represents a GitHub deployment status
type DeploymentStatus struct {
	ID          int64     `json:"id"`
	State       string    `json:"state"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}


// DeploymentSuccessEvent is the schema for the output event
type DeploymentSuccessEvent struct {
	DeploymentID int64     `json:"deployment_id"`
	RepositoryID int64     `json:"repository_id"`
	Environment  string    `json:"environment"`
	Ref          string    `json:"ref"`
	CreatedAt    time.Time `json:"created_at"`
	CompletedAt  time.Time `json:"completed_at"`
} 