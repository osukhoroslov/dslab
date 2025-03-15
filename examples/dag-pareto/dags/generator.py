import pathlib
from wfcommons.wfchef.recipes import *
from wfcommons import WorkflowGenerator

#generator = WorkflowGenerator(EpigenomicsRecipe.from_num_tasks(100000))
#workflow = generator.build_workflow()
#workflow.write_json(pathlib.Path('large-epigenomics-workflow.json'))

generator = WorkflowGenerator(EpigenomicsRecipe.from_num_tasks(1000))
workflow = generator.build_workflow()
workflow.write_json(pathlib.Path('epigenomics-workflow.json'))

#generator = WorkflowGenerator(MontageRecipe.from_num_tasks(5000))
#workflow = generator.build_workflow()
#workflow.write_json(pathlib.Path('montage-workflow.json'))

#generator = WorkflowGenerator(GenomeRecipe.from_num_tasks(10000))
#workflow = generator.build_workflow()
#workflow.write_json(pathlib.Path('genome-workflow.json'))

#generator = WorkflowGenerator(SrasearchRecipe.from_num_tasks(10000))
#workflow = generator.build_workflow()
#workflow.write_json(pathlib.Path('srasearch-workflow.json'))
