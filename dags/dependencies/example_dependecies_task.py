import pendulum
from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "dependencies"],
)
def example_dependencies_task():
    @task(task_id="print_the_context")
    def branching_function_task():
        print("This is a branching function task")

    @task(task_id="say_goodbye")
    def say_goodbye():
        print("Goodbye!")

    @task(task_id="say_hello_friend")
    def hello_friend_task(name: str | None = None):
        if name is None:
            print("Hello, Stranger!")

        print(f"Hello, {name}!")

    branching_function_task() >> [hello_friend_task("Alice"), hello_friend_task()] >> say_goodbye()


example_dependencies_task()
