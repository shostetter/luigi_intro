import luigi
import time

class test(luigi.Task):
    def requires(self):
        """
        Dependancy tasks
        """
        return None

    def output(self):
        """
        target object(s) the task produces when run
        """
        return luigi.LocalTarget('test.txt')

    def run(self):
        time.sleep(15) # for testing to slow it down
        with self.output().open('w') as f:
            f.write('Testing, testing, 1,2,3...')
        time.sleep(15)  # for testing to slow it down

class ChangeSomething(luigi.Task):
    name = luigi.Parameter()

    def requires(self):
        return test()

    def output(self):
        return luigi.LocalTarget(self.input().path + '.name_' + self.name)

    def run(self):
        time.sleep(15)  # for testing to slow it down
        with self.input().open() as infle, self.output().open('w') as outfle:
            text = infle.read()
            text = text.replace('testing', self.name)
            outfle.write(text)
        time.sleep(15)  # for testing to slow it down

if __name__ == '__main__':
    luigi.run()

# run from cmd:
# - start daemon from seperate cmd run: luigid
# - start tasks from cmd run: python luigi_test.py --scheduler-host localhost ChangeSomething --name seth

# browser visualizer: http://localhost:8082