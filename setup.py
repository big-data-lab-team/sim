from setuptools import setup

VERSION = "0.1.0"
DEPS = [
    "boutiques",
    "pybids",
]

setup(name="sim",
      version=VERSION,
      description="A set of MapReduce programs to process brain images",
      url="http://github.com/big-data-lab-team/sim",
      author="Tristan Glatard, Valerie Hayot-Sasson, Lalet Scaria",
      author_email="tristan.glatard@concordia.ca",
      license="GPL3.0",
      packages=["sim"],
      include_package_data=True,
      test_suite="nose.collector",
      tests_require=["nose"],
      setup_requires=DEPS,
      install_requires=DEPS,
      entry_points = {
          "console_scripts": [
              "spark_bids=sim.spark_bids:main",
          ]
      })
