import luigi
import time

class TaskA(luigi.Task):
    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget('task_a')

    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('foo')


class TaskB(luigi.Task):
    def requires(self):
        return TaskA()

    def output(self):
        return luigi.LocalTarget('task_b')

    def run(self):
        time.sleep(15)  # for testing to slow it down
        with self.output().open('w') as outfile:
            outfile.write('bar')


class TaskC(luigi.Task):
    def requires(self):
        return TaskA()  # <-- Notice this dependency!

    def output(self):
        return luigi.LocalTarget(self.input().path + '.task_c')

    def run(self):
        time.sleep(15)  # for testing to slow it down
        with self.input().open() as infile, self.output().open('w') as outfile:
            for line in infile:
                outfile.write(line)


class TaskD(luigi.Task):
    def requires(self):
        return TaskB()

    def output(self):
        return luigi.LocalTarget('task_d')

    def run(self):
        time.sleep(15)  # for testing to slow it down
        with self.output().open('w') as outfile:
            outfile.write('dar')
# Let's create an own "copy" of TaskC, that depends on TaskB instead of TaskA:


class MyTaskC(TaskC):
    def requires(self):
        return TaskB()  # <-- Notice how we switched the dependency in TaskC!


class MyTaskD(TaskD):
    def requires(self):
        return MyTaskC()
    
    def output(self):
        return luigi.LocalTarget(self.input().path + '.task_d')


if __name__ == '__main__':
    luigi.run()