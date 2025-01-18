from airflow.decorators import task, dag
from datetime import datetime


@dag(schedule_interval=None, start_date=datetime(2023, 1, 1), catchup=False)
def example_bash_operator():

    @task
    def upstream_task():
        dog_owner_data = {
            "names": ["Trevor", "Grant", "Marcy", "Carly", "Philip"],
            "dogs": [1, 2, 2, 0, 4],
        }
        return dog_owner_data

    @task.bash
    def bash_task(dog_owner_data):
        names_of_dogless_people = []
        for name, dog in zip(dog_owner_data["names"], dog_owner_data["dogs"]):
            if dog < 1:
                names_of_dogless_people.append(name)

        if names_of_dogless_people:
            if len(names_of_dogless_people) == 1:
                # this bash command is executed if only one person has no dog
                return f'echo "{names_of_dogless_people[0]} urgently needs a dog!"'
            else:
                names_of_dogless_people_str = " and ".join(names_of_dogless_people)
                # this bash command is executed if more than one person has no dog
                return f'echo "{names_of_dogless_people_str} urgently need a dog!"'
        else:
            # this bash command is executed if everyone has at least one dog
            return f'echo "All good, everyone has at least one dog!"'

    # Setting up task dependencies
    dog_owner_data = upstream_task()
    bash_task(dog_owner_data)


# Instantiate the DAG
example_bash_operator()
