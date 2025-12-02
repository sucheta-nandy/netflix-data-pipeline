"""
Validation rules for Netflix metadata
Defines schema and quality constraints
"""

# Expected schema for Netflix data
NETFLIX_SCHEMA = {
    'show_id': {
        'type': 'string',
        'nullable': False,
        'description': 'Unique identifier for the show'
    },
    'type': {
        'type': 'string',
        'nullable': False,
        'allowed_values': ['Movie', 'TV Show'],
        'description': 'Type of content'
    },
    'title': {
        'type': 'string',
        'nullable': False,
        'min_length': 1,
        'max_length': 500,
        'description': 'Title of the content'
    },
    'director': {
        'type': 'string',
        'nullable': True,
        'description': 'Director name(s)'
    },
    'cast': {
        'type': 'string',
        'nullable': True,
        'description': 'Cast members'
    },
    'country': {
        'type': 'string',
        'nullable': True,
        'description': 'Production country'
    },
    'date_added': {
        'type': 'date',
        'nullable': True,
        'description': 'Date added to Netflix'
    },
    'release_year': {
        'type': 'integer',
        'nullable': False,
        'min': 1900,
        'max': 2025,
        'description': 'Year of release'
    },
    'rating': {
        'type': 'string',
        'nullable': True,
        'allowed_values': [
            'G', 'PG', 'PG-13', 'R', 'NC-17',
            'TV-Y', 'TV-Y7', 'TV-G', 'TV-PG', 'TV-14', 'TV-MA',
            'NR', 'UR', 'Not Rated', 'Unknown'
        ],
        'description': 'Content rating'
    },
    'duration': {
        'type': 'string',
        'nullable': False,
        'description': 'Duration (minutes or seasons)'
    },
    'listed_in': {
        'type': 'string',
        'nullable': False,
        'description': 'Genres/categories'
    },
    'description': {
        'type': 'string',
        'nullable': True,
        'min_length': 10,
        'max_length': 5000,
        'description': 'Content description'
    }
}

# Transformed schema (after ETL)
TRANSFORMED_SCHEMA = {
    **NETFLIX_SCHEMA,
    'primary_country': {
        'type': 'string',
        'nullable': True,
        'description': 'Primary production country'
    },
    'duration_value': {
        'type': 'integer',
        'nullable': False,
        'min': 1,
        'max': 1000,
        'description': 'Numeric duration value'
    },
    'duration_unit': {
        'type': 'string',
        'nullable': False,
        'allowed_values': ['min', 'Season', 'Unknown'],
        'description': 'Duration unit'
    },
    'genres': {
        'type': 'array',
        'nullable': False,
        'description': 'Array of genres'
    },
    'genre_count': {
        'type': 'integer',
        'nullable': False,
        'min': 1,
        'max': 10,
        'description': 'Number of genres'
    },
    'age_category': {
        'type': 'string',
        'nullable': False,
        'allowed_values': ['Kids', 'Older Kids', 'Teens', 'Adults', 'Not Rated'],
        'description': 'Age category'
    },
    'content_age': {
        'type': 'integer',
        'nullable': False,
        'min': 0,
        'max': 125,
        'description': 'Years since release'
    },
    'is_recent': {
        'type': 'boolean',
        'nullable': False,
        'description': 'Whether content is recent'
    }
}

# Data quality rules
QUALITY_RULES = {
    'completeness': {
        'required_columns': ['show_id', 'type', 'title', 'release_year', 'duration'],
        'max_null_rate': 0.05  # Max 5% nulls for non-required columns
    },
    'uniqueness': {
        'unique_columns': ['show_id']
    },
    'consistency': {
        'type_movie_duration_unit': {
            'condition': "type = 'Movie' AND duration_unit = 'min'",
            'description': 'Movies should have duration in minutes'
        },
        'type_tvshow_duration_unit': {
            'condition': "type = 'TV Show' AND duration_unit = 'Season'",
            'description': 'TV Shows should have duration in seasons'
        }
    },
    'validity': {
        'release_year_reasonable': {
            'column': 'release_year',
            'min': 1900,
            'max': 2025,
            'description': 'Release year should be reasonable'
        },
        'duration_positive': {
            'column': 'duration_value',
            'min': 1,
            'description': 'Duration should be positive'
        }
    }
}

# Statistical thresholds
STATISTICAL_THRESHOLDS = {
    'outliers': {
        'duration_value': {
            'method': 'iqr',
            'multiplier': 3.0
        },
        'content_age': {
            'method': 'iqr',
            'multiplier': 3.0
        }
    },
    'distributions': {
        'type': {
            'expected_ratio': {
                'Movie': (0.6, 0.8),  # 60-80% movies
                'TV Show': (0.2, 0.4)  # 20-40% TV shows
            }
        }
    }
}
