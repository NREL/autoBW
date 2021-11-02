import brightway2 as bw
import yaml
import logging

class Project:
    def __init__(
            self,
            project
    ):
        """
        Parameters
        ----------
        project
            Dictionary of parameters for instantiating a new Project

        Return

        """

        bw.projects.set_current(project.get('name'))

        # Run the setup to get the biosphere database
        bw.bw2setup()