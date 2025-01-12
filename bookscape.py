import streamlit as st
import pymysql
import requests
import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from matplotlib import pyplot as plt
import pandas as pd
import seaborn as sns
from PIL import Image

# Database connection details
DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "Raghavi@1598",
    "database": "bookapi",
    "port": 3306,
}

# Function to establish a database connection
def get_connection():
    try:
        connection = pymysql.connect(
            host=DB_CONFIG["host"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
            port=DB_CONFIG["port"],
        )
        return connection
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        return None

# Utility function to create the books table
def create_books_table():
    try:
        connection = pymysql.connect(
            host=DB_CONFIG["host"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
        )
        cursor = connection.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS books_extract(
            id INT AUTO_INCREMENT PRIMARY KEY,
            book_title VARCHAR(255),
            book_authors VARCHAR(255),
            publisher VARCHAR(255),
            year VARCHAR(4),
            page_count INT,
            avg_rating DECIMAL(3, 2),
            ratings_count INT
        )
        """
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        connection.close()
    except pymysql.MySQLError as e:
        st.error(f"Error creating table: {e}")

# Extract book details from the API response
def extract_book_details(items):
    books = []
    for item in items:
        volume_info = item.get("volumeInfo", {})
        books.append({
            "book_title": volume_info.get("title", "N/A"),
            "book_authors": ", ".join(volume_info.get("authors", [])),
            "publisher": volume_info.get("publisher", "N/A"),
            "year": volume_info.get("publishedDate", "N/A").split("-")[0],
            "page_count": volume_info.get("pageCount", 0),
            "avg_rating": volume_info.get("averageRating", None),
            "ratings_count": volume_info.get("ratingsCount", None),
        })
    return books

# Fetch books data from the Google Books API
def fetch_books(search_key, max_results=100):
    books_data = []
    start_index = 0
    API_KEY = "AIzaSyDfdDGsTfMa9zXqr3PliFemr_4jge0dPos"
    API_URL = "https://www.googleapis.com/books/v1/volumes"

    while start_index < max_results:
        params = {
            "q": search_key,
            "startIndex": start_index,
            "maxResults": 40,
            "key": API_KEY,
        }
        response = requests.get(API_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            items = data.get("items", [])
            if not items:
                break
            books_data.extend(extract_book_details(items))
            start_index += 40
        else:
            st.error(f"API error: {response.status_code}")
            break
    return books_data

# Database engine connection
def get_engine():
    try:
        password_encoded = urllib.parse.quote(DB_CONFIG['password'])  # Encode password
        connection_string = (
            f"mysql+pymysql://{DB_CONFIG['user']}:{password_encoded}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        return create_engine(connection_string)
    except SQLAlchemyError as e:
        st.error(f"Database connection error: {e}")
        return None

# Insert books into the database
def insert_books_to_db(books_df):
    engine = get_engine()
    if engine:
        try:
            books_df.to_sql(
                name="books_extract",
                con=engine,
                if_exists="replace",
                index=False,
                chunksize=1000,
            )
            st.success("Books successfully inserted into the database!")
        except SQLAlchemyError as e:
            st.error(f"Error inserting data: {e}")
    else:
        st.error("Database connection failed.")

# Streamlit App
st.title("BookScape Explorer")

# Sidebar Navigation
choice = st.sidebar.radio(
    label="Choose an option",
    options=("BookScape Explorer", "Questions", "Extraction")
)

if choice == "BookScape Explorer":
    img_path = "E:\\image.jpg\\download.jpeg"  # Update this path as needed
    try:
        img = Image.open(img_path)
        st.image(
            img,
            caption="Bookscape is a tech-driven, reader-centric online bookstore with 1 million+ titles from 700+ publishers. It is custom designed to connect content creators and publishers to readers seamlessly.",
            width=400,
            channels="RGB"
        )
    except Exception as e:
        st.error(f"Error loading image: {e}")

elif choice == "Questions":
    st.subheader("Query Selection")
    option = st.selectbox(
        "Choose Your Query:",
        options=[
            "Check Availability of eBooks vs Physical Books",
            "Find the Publisher with the Most Books Published",
            "Identify the Publisher with the Highest Average Rating",
            "Get the Top 5 Most Expensive Books by Retail Price",
            "Find Books Published After 2010 with at Least 500 Pages",
            "List Books with Discounts Greater than 20%",
            "Find the Average Page Count for eBooks vs Physical Books",
            "Find the Top 3 Authors with the Most Books",
            "List Publishers with More than 10 Books",
            "Find the Average Page Count for Each Category",
            "Retrieve Books with More than 3 Authors",
            "Books with Ratings Count Greater Than the Average",
            "Books with the Same Author Published in the Same Year",
            "Books with a Specific Keyword in the Title",
            "Year with the Highest Average Book Price",
            "Count Authors Who Published 3 Consecutive Years",
            "Authors Published in the Same Year under Different Publishers",
            "Average Price of eBooks vs Physical Books",
            "Outlier Books by Average Rating",
            "Publisher with Highest Average Rating (More Than 10 Books)"
        ]
    )

    SQL_QUERIES = {
        "Check Availability of eBooks vs Physical Books": """
            SELECT isEbook, COUNT(*) AS book_count
            FROM books
            GROUP BY isEbook;
    """
        ,
        "Find the Publisher with the Most Books Published": """
            select publisher, count(*) as book_count
            from books        
            group by publisher
            order by book_count desc ;
    """,
    "Identify the Publisher with the Highest Average Rating": """
            select publisher ,avg(averageRating) as averageRating
            from books
            group by publisher
            order by averageRating desc;

    """,
    "Get the Top 5 Most Expensive Books by Retail Price": """
            select book_title, amount_retailPrice from books
            order  by amount_retailPrice desc limit 5;
    """,
    "Find Books Published After 2010 with at Least 500 Pages": """
            select book_title,publisher,pageCount,year from books
            where year>2010 and pageCount>=500
            ORDER BY pageCount DESC;
    """,
    "List Books with Discounts Greater than 20%": """
            select book_title,amount_listPrice AS retail_price,amount_retailPrice AS sale_price from books
            where (amount_listPrice - amount_retailPrice) / amount_listPrice > 0.2;
    """,
    "Find the Average Page Count for eBooks vs Physical Books": """
            select isEbook, avg(pageCount)  AS pageCount from books
            group by isEbook;
    """,
    "Find the Top 3 Authors with the Most Books": """
            select book_authors, count(*) as  book_count from books
            group by book_authors 
            order by  book_count 
            limit 3;
    """,
    "List Publishers with More than 10 Books": """
            select publisher ,count(*) as  book_count from books
            group by publisher having book_count >10 
            ORDER BY book_count DESC;;
    """,
    "Find the Average Page Count for Each Category": """
            select categories as category , avg(pagecount) as pagecount from books 
            group by categories;
    """,
    "Retrieve Books with More than 3 Authors": """
            select book_title,count(book_authors) as book_authors from books 
            where book_authors like '%,%' 
            group by book_title having book_authors >3;
    """,
    "Books with Ratings Count Greater Than the Average": """
            select book_title, ratingscount from books
            where ratingscount > (select avg(ratingscount) from books);
    """,
    "Books with the Same Author Published in the Same Year": """
            select book_title,book_authors,year from books
            where (book_authors, year) IN (
                        SELECT book_authors,year
                        FROM books
                        GROUP BY book_authors,year
                        HAVING COUNT(*) > 1
                        order by book_authors desc
        );
    """,
    "Books with a Specific Keyword in the Title": """
            select book_title, search_key, avg(pageCount) as average_page_count
            from books
            group by book_title, search_key;
    """,
    "Year with the Highest Average Book Price": """
            select year, AVG(amount_retailPrice) AS avg_price from books
            group by year
            order by avg_price DESC
            limit 1;
    """,
    "Count Authors Who Published 3 Consecutive Years": """
            SELECT book_authors, year  
            FROM books 
            WHERE year BETWEEN YEAR(CURDATE()) - 2 AND YEAR(CURDATE())
            ORDER BY book_authors, year;
    """,
    "Authors Published in the Same Year under Different Publishers": """
        SELECT book_authors, year, COUNT(DISTINCT publisher) AS publisher_count
        FROM books
        GROUP BY book_authors, year
        HAVING publisher_count > 1;
    """,
    "Average Price of eBooks vs Physical Books": """
        SELECT 
            AVG(CASE WHEN isEbook = 1 THEN amount_retailPrice ELSE NULL END) AS avg_ebook_price,
            AVG(CASE WHEN isEbook = 0 THEN amount_retailPrice ELSE NULL END) AS avg_physical_price
        FROM books;
    """,
    "Outlier Books by Average Rating": """
        SELECT book_title, averageRating
        FROM books;
    """,
    "Publisher with Highest Average Rating (More Than 10 Books)": """
        SELECT publisher, AVG(averageRating) AS avg_rating, COUNT(*) AS book_count
        FROM books
        GROUP BY publisher
        HAVING book_count > 10
        ORDER BY avg_rating DESC
        LIMIT 10;
    """
    }

    query = SQL_QUERIES.get(option)

    if query:
        with st.spinner("Executing query..."):
            connection = get_connection()
            if connection:
                try:
                # Execute the query and load the result into a DataFrame
                    df = pd.read_sql(query, connection)
                    if not df.empty:
                        st.dataframe(df)  # Display the data in Streamlit
                         # Plot the data if it's the "Check Availability" query
                        if option == "Check Availability of eBooks vs Physical Books":
                            categories = df['isEbook'].replace({1: 'eBooks', 0: 'Physical Books'})
                            counts = df['book_count']
                            width=400
                            plt.bar(categories, counts)
                            plt.title("Availability of eBooks vs Physical Books")
                            plt.xlabel("Book Type")
                            plt.ylabel("Count")
                            st.pyplot(plt)  # Display the plot in Streamlit
                            plt.clf()  # Clear the plot

                        if option == "Find the Publisher with the Most Books Published":
                        # Plot the bar chart
                            plt.figure(figsize=(10, 6))
                            plt.bar(df['publisher'], df['book_count'], color='skyblue')
                            plt.title("Publishers with the Most Books Published")
                            plt.xlabel("Publisher")
                            plt.ylabel("Book Count")
                            plt.xticks(rotation=45, ha='right')
                            st.pyplot(plt)  # Display the plot in Streamlit
                            plt.clf()  # Clear the plot after rendering

                        if option == "Identify the Publisher with the Highest Average Rating":
                        # Plot the line chart
                            plt.figure(figsize=(10, 6))
                            plt.plot( df['averageRating'], marker='o', linestyle='-', color='blue')
                            plt.title("Publishers by Average Rating")
                            plt.xlabel("Publisher")
                            plt.ylabel("Average Rating")
                            plt.xticks(rotation=45, ha='right')
                            plt.grid(True)
                            st.pyplot(plt)  # Display the plot in Streamlit
                            plt.clf()  # Clear the plot after rendering

                        if option == "Get the Top 5 Most Expensive Books by Retail Price":
                        # Plot the bar chart
                            plt.figure(figsize=(10, 6))
                            plt.barh(df['book_title'], df['amount_retailPrice'], color='orange')
                            plt.title("Top 5 Most Expensive Books by Retail Price")
                            plt.xlabel("Retail Price (in currency)")
                            plt.ylabel("Book Title")
                            plt.gca().invert_yaxis()  # Invert y-axis to have the highest price on top
                            plt.tight_layout()
                            st.pyplot(plt)  # Display the plot in Streamlit
                            plt.clf()  # Clear the plot after rendering


                        if option == "Find Books Published After 2010 with at Least 500 Pages":
                            if not df.empty:
                            # Sort the DataFrame by pageCount (if not already sorted by SQL)
                                df = df.sort_values(by="pageCount", ascending=False)

                                # Plot the bar chart
                                plt.figure(figsize=(12, 6))
                                plt.bar(df['year'], df['pageCount'], color='skyblue')
                                plt.title("Books Published After 2010 with at Least 500 Pages (Descending Order)")
                                plt.xlabel("year")
                                plt.ylabel("Page Count")
                                plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for readability
                                plt.tight_layout()  # Adjust layout to prevent overlap
                                st.pyplot(plt)  # Display the plot in Streamlit
                                plt.clf()  # Clear the plot after rendering

                        if option == "Find the Average Page Count for eBooks vs Physical Books":
                           if not df.empty:
        # Map isEbook values to readable labels
                                df['Book Type'] = df['isEbook'].map({0: 'Physical Books', 1: 'eBooks'})

        # Create Pie Chart
                                plt.figure(figsize=(8, 8))
                                plt.pie(df['pageCount'], labels=df['Book Type'], autopct='%1.1f%%', colors=['lightblue', 'lightgreen'])
                                plt.title("Average Page Count for eBooks vs Physical Books")

        # Display the chart in Streamlit
                                st.pyplot(plt)
                                plt.clf()  # Clear the plot after rendering

                        if option == "Find the Top 3 Authors with the Most Books":
                            if not df.empty:
        # Plot the bar chart
                                plt.figure(figsize=(10, 6))
                                plt.bar(df['book_authors'], df['book_count'], color='skyblue')
                                plt.title("Top 3 Authors with the Most Books")
                                plt.xlabel("Authors")
                                plt.ylabel("Book Count")
                                plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for readability
                                plt.tight_layout()

        # Display the plot in Streamlit
                                st.pyplot(plt)
                                plt.clf()  # Clear the plot after rendering

                        if option == "List Publishers with More than 10 Books":
                            if not df.empty:
        # Plot the bar chart
                                plt.figure(figsize=(12, 6))
                                plt.bar(df['publisher'], df['book_count'], color='skyblue')
                                plt.title("Publishers with More than 10 Books")
                                plt.xlabel("Publisher")
                                plt.ylabel("Book Count")
                                plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for better readability
                                plt.tight_layout()

        # Display the plot in Streamlit
                                st.pyplot(plt)
                                plt.clf()  # Clear the plot after rendering

                        if option == "Retrieve Books with More than 3 Authors":
                            if not df.empty:
        # Plot the bar chart
                                plt.figure(figsize=(12, 6))
                                plt.bar(df['book_title'], df['book_authors'], color='skyblue')
                                plt.title("Books with More Than 3 Authors")
                                plt.xlabel("Book Title")
                                plt.ylabel("Number of Authors")
                                plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for readability
                                plt.tight_layout()

        # Display the plot in Streamlit
                                st.pyplot(plt)
                                plt.clf()  # Clear the plot after rendering

                        if option == "Books with a Specific Keyword in the Title":
                            if not df.empty:
        # Group the data by 'search_key' and sum the average page count
                                grouped_data = df.groupby('search_key')['average_page_count'].mean()

        # Plot the pie chart
                                plt.figure(figsize=(8, 8))
                                plt.pie(grouped_data, labels=grouped_data.index, autopct='%1.1f%%', colors=plt.cm.Paired.colors)
                                plt.title("Average Page Count for Books with Specific Keywords in the Title")
        
        # Display the pie chart in Streamlit
                                st.pyplot(plt)
                                plt.clf()  # Clear the plot after rendering

                        if option == "Count Authors Who Published 3 Consecutive Years":
                            if not df.empty:
        # Count the number of occurrences of each author
                                author_counts = df['book_authors'].value_counts()

        # Filter authors who have published in exactly 3 consecutive years
                                authors_with_3_years = author_counts[author_counts == 3]

        # Plot the pie chart if we have authors who published in 3 years
                                if not authors_with_3_years.empty:
                                        plt.figure(figsize=(8, 8))
                                        plt.pie(authors_with_3_years, labels=authors_with_3_years.index, autopct='%1.1f%%', colors=plt.cm.Paired.colors)
                                        plt.title("Authors Who Published in 3 Consecutive Years")

            # Display the pie chart in Streamlit
                                        st.pyplot(plt)
                                        plt.clf()  # Clear the plot after rendering
                                else:
                                    st.info("No authors found who published in exactly 3 consecutive years.")
        
                        if option == "Outlier Books by Average Rating":
    # Check if df is not empty (after running your SQL query)
                            if not df.empty:
        # Plotting the boxplot of averageRating
                                plt.figure(figsize=(10, 6))
                                sns.boxplot(x=df['averageRating'], color='green')
                                plt.title('Boxplot of Book Ratings with Outliers')
                                plt.xlabel('Average Rating')

        # Use st.pyplot to display the plot in Streamlit
                                st.pyplot(plt)

        # Optionally, print the DataFrame to inspect
                                print(df)


                        if option == "Publisher with Highest Average Rating (More Than 10 Books)":
    # Assume df contains the result of the SQL query
    # Example: df = your_sql_query_result

    # Check if the DataFrame is not empty
                            if not df.empty:
        # Plotting a line chart for average rating per publisher
                                plt.figure(figsize=(10, 6))
                                sns.lineplot(x='publisher', y='avg_rating', data=df, marker='o', color='green')

        # Title and labels
                                plt.title('Publisher with Highest Average Rating (More Than 10 Books)')
                                plt.xlabel('Publisher')
                                plt.ylabel('Average Rating')

        # Rotate x-axis labels for readability
                                plt.xticks(rotation=45, ha='right')

        # Display the plot in Streamlit
                                st.pyplot(plt)

        # Optionally, print the DataFrame to inspect the data
                                print(df)        




                    else:
                        st.info("No results found.")
                except Exception as e:
                            st.error(f"Error executing query: {e}")
                finally:
                    connection.close()

    else:
        st.warning("Please select a valid query.")

elif choice == "Extraction":
    st.write("You chose Extraction. Fetch and save books using the Google Books API.")

    search_query = st.text_input("Enter a search term for books:")

    if search_query:
        if st.button("Fetch and Save Books"):
            books_data = fetch_books(search_query)
            if books_data:
                books_df = pd.DataFrame(books_data)
                st.write("Fetched Books Data:")
                st.dataframe(books_df)
                insert_books_to_db(books_df)
            else:
                st.info("No books found for the given search term.")
    else:
        st.warning("Please enter a search term.")

if __name__ == "__main__":
    create_books_table()
