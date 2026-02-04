from quality.context import get_ge_context

def run_checkpoint(name: str):
    context = get_ge_context()
    return context.run_checkpoint(checkpoint_name=name)