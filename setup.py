from setuptools import setup

VERSION = "0.1.3"
DEPS = [
    "boutiques=>0.5.3",
    "pybids",
    "pyspark",
    "pytest-runner"
]

setup(name="simtools",
      version=VERSION,
      description="A set of MapReduce programs to process brain images",
      url="http://github.com/big-data-lab-team/sim",
      author="Tristan Glatard, Valerie Hayot-Sasson, Lalet Scaria",
      author_email="tristan.glatard@concordia.ca",
      license="GPL3.0",
      packages=["sim"],
      include_package_data=True,
      test_suite="pytest",
      tests_require=["pytest"],
      setup_requires=DEPS,
      install_requires=DEPS,
      entry_points = {
          "console_scripts": [
              "spark_bids=sim.spark_bids:main",
          ]
      })
