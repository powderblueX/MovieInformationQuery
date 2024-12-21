import csv
from imdb import IMDb
import pandas as pd

# 创建 IMDb 对象
ia = IMDb()

# 读取 CSV 文件
input_file = r'D:\vscode\DataScrapingbyBeautifulSoup\Genres-Movies\filtered_directors.csv'
# 创建一个新的 CSV 文件并写入标题行
output_file = r'D:\vscode\DataScrapingbyBeautifulSoup\Genres-Movies\filtered_Info.csv'

# 定义一个函数根据标题获取电影的 IMDb ID 并获取电影的 Genres, Directors, All Actors
def get_movie_info_by_title(title):
    try:
        # 搜索电影
        movies = ia.search_movie(title)
        
        # 确保搜索到结果并且获取第一个电影
        if movies:
            movie = movies[0]  # 选择搜索结果的第一个电影
            movie_id = movie.movieID  # 获取电影的 IMDb ID
            
            # 获取电影的详细信息
            movie = ia.get_movie(movie_id)
            
            # 获取 Genres
            genres = movie.get('genres', [])
            
            # 获取 Directors
            directors = movie.get('directors', [])
            directors_list = [director['name'] for director in directors]
            
            # 获取 All Actors
            starring_actors = movie.get('cast', [])
            all_actors = [actor['name'] for actor in starring_actors]
            
            # 返回 Genres, Directors, All Actors
            return ', '.join(genres), ', '.join(directors_list), ', '.join(all_actors)
        else:
            print(f"Movie '{title}' not found.")
            return None, None, None
    except Exception as e:
        print(f"Error fetching data for movie '{title}': {e}")
        return None, None, None



def write_to_file(movie_info):
    # 打开输出文件并写入标题
    with open(output_file, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Productid', 'Movie Title', 'Genres', 'Directors', 'All Actors']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writerow(movie_info)

# 遍历 CSV 文件的每一行
df = pd.read_csv(input_file)
for index, row in df.iterrows():
    productid = row['Productid']  # 获取 Productid
    movie_title = row['Movie Title']  # 获取电影标题
    print(f"Processing '{movie_title}'...")
        
    # 获取电影的 IMDb 信息
    genres, directors, all_actors = get_movie_info_by_title(movie_title)
            
    movie_info = {
        'Productid': productid,
        'Movie Title': movie_title,
        'Genres': genres,
        'Directors': directors,
        'All Actors': all_actors
    }
    write_to_file(movie_info)


print(f"Data scraping complete. Saved to {output_file}")
