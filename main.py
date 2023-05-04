# from typing import Union
from typing import Any, Union
from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel
from starlette.responses import RedirectResponse
from pymongo import MongoClient

app = FastAPI()


# connect to MongoDB database by PyMongo
try:
    client = MongoClient("44.201.157.66", 27018)
    db = client.get_database("moviedb")
    print("Connected to MongoDB, collectionName = " + db.list_collection_names()[0])
except Exception as e:
    print("Failed to connect to MongoDB database")
    print(e)
    exit(-1)


# redirect the home page to swagger docs page
@app.get("/", include_in_schema=False)
def redirect_to_swagger_page():
    return RedirectResponse("/docs")


# define general error message format
class ValidatorError(BaseModel):
    msg: str = ''
    data: Any = None


# override default error response schema in FastAPI docs
error_response = {
    422: {
        "description": "Error"
    }
}


# override default OK response schema in FastAPI docs
class GeneralOKResponse(BaseModel):
    status = "Succeed"
    data = {}


# function 1
# Add a new movie review
class AddMovieReviewRequest(BaseModel):
    movie_item_id: int
    new_review_content: str

@app.post("/addNewMovieReview", response_model=GeneralOKResponse, status_code=200, responses=error_response)
def add_new_Movie_review(add_movie_review_request: AddMovieReviewRequest):
    result = db.movie.update_one({"item_id": add_movie_review_request.movie_item_id}, {"$push": {"reviews": add_movie_review_request.new_review_content}})
    return {
        "status": "Succeed",
        "data": {
            "modified_count": result.modified_count
        }
    }


# function 2
# Show all existing reviews for a movie
@app.post("/showReviews/{movie_item_id}")
def show_all_movie_reviews(movie_item_id: int):
    result = db.movie.find_one({"item_id": movie_item_id}, {"reviews": 1, "_id": 0})
    return {
        "status": "Succeed",
        "data": result
    }


# function 3
# get basic info for a movie (excluding tags and reviews)
@app.get("/basicInfo/{movie_item_id}")
def get_basic_movie_info(movie_item_id: int):
    result = db.movie.find_one({"item_id": movie_item_id}, {"_id": 0, "tags": 0, "reviews": 0})
    return {
        "status": "Succeed",
        "data": result
    }

# function 4
# Update an existing movie basic info
class UpdateMovieInfoRequest(BaseModel):
    movie_item_id: int
    title: Union[str, None] = None  # title is either str or None, default = None
    directedBy: Union[str, None] = None
    starring: Union[str, None] = None

@app.put("/updateMovieInfo")
def update_movie_info(update_movie_info_request: UpdateMovieInfoRequest):
    updateData = {}
    if update_movie_info_request.title:
        updateData["title"] = update_movie_info_request.title
    if update_movie_info_request.title:
        updateData["directedBy"] = update_movie_info_request.directedBy
    if update_movie_info_request.title:
        updateData["starring"] = update_movie_info_request.starring

    result = db.movie.update_one({"item_id": update_movie_info_request.movie_item_id}, {"$set": updateData})
    return {
        "status": "Succeed",
        "data": {
            "modified_count": result.modified_count
        }
    }


# function 5
# Find movies by title
@app.get("/findMovieByTitle/{movie_title}")
def find_movie_by_name(movie_title: str):
    results = db.movie.find({"title": movie_title}, {"_id": 0})
    results_list = [i for i in results]
    return {
        "status": "Succeed",
        "data": {
            "results": results_list
        }
    }


# function 6
# Find top-rated movies' titles and average ratings
@app.get("/findTopRatedMoviesTitles/{page}/{page_size}")
def find_top_rated_movies(page: int, page_size: int):
    # if page <= 0, return data for page == 1
    if page <= 0:
        page = 1

    # return 100 enties at most
    if page_size > 100:
        page_size = 100

    # number of data we need to skip (will be in previous pages)
    skip = (page - 1) * page_size

    results = db.movie.find({}, {"title": 1, "avgRating": 1, "_id": 0}).sort("avgRating", -1).skip(skip).limit(page_size)
    results_list = [i for i in results]
    return {
        "status": "Succeed",
        "data": {
            "results": results_list
        }
    }


# function 7
# Find movies's title, rating, and directedBy with a specific director
@app.get("/findMoviesByDirector/{director_name}")
def find_movies_by_director(director_name: str):
    results = db.movie.find({"directedBy": director_name}, {"_id": 0, "title": 1, "avgRating": 1, "directedBy": 1}).sort("avgRating", -1)
    results_list = [i for i in results]
    return {
        "status": "Succeed",
        "data": {
            "results": results_list
        }
    }

# function 8
# Find movies' title and ratings with a rating in a range sorted by avgRating
@app.get("/findMoviesBetterThanRating/{upper_range}/{lower_range}/{page}/{page_size}")
def find_movies_Better_than_rating(upper_range: float, lower_range: float, page: int, page_size: int):
    # if page <= 0, return data for page == 1
    if page <= 0:
        page = 1

    # return 100 enties at most
    if page_size > 100:
        page_size = 100
    
    # number of data we need to skip (will be in previous pages)
    skip = (page - 1) * page_size

    # fix upper_range and low_range if invalid
    if upper_range > 5:
        upper_range = 5
    
    if upper_range < 0:
        upper_range = 0

    if lower_range > 5:
        lower_range = 5
    
    if lower_range < 0:
        lower_range = 0

    # if upper range < lower range (invalid), return all movie sorted by average rating
    if upper_range < lower_range:
        upper_range = 5
        lower_range = 0
    
    results = db.movie.find({"avgRating": {"$gte": lower_range, "$lte": upper_range}}, {"_id": 0, "avgRating": 1, "title": 1}).skip(skip).limit(page_size)
    results_list = [i for i in results]
    return {
        "status": "Succeed",
        "data": {
            "results": results_list
        }
    }

# function 9
# Find title and ratings of movies containing a specific word, sorting by rating
@app.get("/findMovieContains/{keyword}/{page}/{page_size}")
def find_movie_contains(keyword: str, page: int, page_size: int):
     # if page <= 0, return data for page == 1
    if page <= 0:
        page = 1

    # return 100 enties at most
    if page_size > 100:
        page_size = 100
    
    # number of data we need to skip (will be in previous pages)
    skip = (page - 1) * page_size

    results = db.movie.find({"title": {"$regex": keyword}}, {"_id": 0, "title": 1, "avgRating": 1}).sort("avgRating", -1).skip(skip).limit(page_size)
    results_list = [i for i in results]
    return {
        "status": "Succeed",
        "data": {
            "results": results_list
        }
    }

# function 10
# Find title and rating of movies with more than an average rating, sorting by rating
@app.get("/findMoviesBetterThanRating/{rating}/{page}/{page_size}")
def find_movies_Better_than_rating(rating: float, page: int, page_size: int):
    # if page <= 0, return data for page == 1
    if page <= 0:
        page = 1

    # return 100 enties at most
    if page_size > 100:
        page_size = 100
    
    # number of data we need to skip (will be in previous pages)
    skip = (page - 1) * page_size

    # fix upper_range and low_range if invalid
    if rating > 5:
        rating = 5

    if rating < 0:
        rating = 0
    
    results = db.movie.find({"avgRating": {"$gt": rating}}, {"_id": 0, "avgRating": 1, "title": 1}).skip(skip).limit(page_size)
    results_list = [i for i in results]
    return {
        "status": "Succeed",
        "data": {
            "results": results_list
        }
    }


# function 10
# Find name and tags movies with a specific tag
@app.get("/findMoviesWithTag/{tag}/{page}/{page_size}")
def find_movies_with_tag(tag: str, page: int, page_size: int):
    # if page <= 0, return data for page == 1
    if page <= 0:
        page = 1

    # return 100 enties at most
    if page_size > 100:
        page_size = 100
    
    # number of data we need to skip (will be in previous pages)
    skip = (page - 1) * page_size

    results = db.movie.find({"tags": tag}, {"_id": 0, "title": 1, "tags": 1}).skip(skip).limit(page_size)
    results_list = [i for i in results]
    return {
        "status": "Succeed",
        "data": {
            "results": results_list
        }
    }

# function 12
# Find movies of specific stars
@app.get("/findMoviesWithStar/{star}/{page}/{page_size}")
def find_movies_with_star(star: str, page: int, page_size: int):
    # if page <= 0, return data for page == 1
    if page <= 0:
        page = 1

    # return 100 enties at most
    if page_size > 100:
        page_size = 100
    
    # number of data we need to skip (will be in previous pages)
    skip = (page - 1) * page_size

    # fix blank space in star
    star = star.split(" ")
    star = "\s+".join(star)

    results = db.movie.find({"starring": {"$regex": star}}, {"_id": 0, "title": 1, "starring": 1}).skip(skip).limit(page_size)
    results_list = [i for i in results]
    return {
        "status": "Succeed",
        "data": {
            "results": results_list
        }
    }



# function 13
# get all tags of a movie
@app.get("/displayTags/{movie_item_id}")
def display_tags(movie_item_id: int):
    result = db.movie.find_one({"item_id": movie_item_id}, {"_id": 0, "title": 1, "tags": 1})
    return {
        "status": "Succeed",
        "data": result
    }

# function 14
# add a tag to a movie
class AddTagRequest(BaseModel):
    movie_item_id: int
    new_tag_name: str

@app.post("/addTag")
def add_tag(add_tag_request: AddTagRequest):
    result = db.movie.update_one({"item_id": add_tag_request.movie_item_id}, {"$push": {"tags": add_tag_request.new_tag_name}})
    return {
        "status": "Succeed",
        "data": {
            "modified_count": result.modified_count
        }
    }


# function 15
# remove a tag from a movie
class DeleteTagRequest(BaseModel):
    movie_item_id: int
    tag_name: str

@app.delete("/deleteTag")
def delete_tag(delete_tag_request: DeleteTagRequest):
    result = db.movie.update_one({"item_id": delete_tag_request.movie_item_id}, {"$pull": {"tags": delete_tag_request.tag_name}})
    return {
        "status": "Succeed",
        "data": {
            "modified_count": result.modified_count
        }
    }
