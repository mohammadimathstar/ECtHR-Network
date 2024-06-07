
import os
import argparse
import pickle


def get_builder_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser('Create The Citation Network of European Court of Human Rights.')
    parser.add_argument('--doctype',
                        type=str,
                        default='JUDGMENTS',
                        help='The type of legal document: a) JUDGMENTS, b) DECISIONS.')
    parser.add_argument('--graph_name',
                        type=str,
                        default='graph',
                        help='The name to save the graph.')
    parser.add_argument('--csv_dir',
                        type=str,
                        default='./data/download',
                        help='The directory to read meta-data (which is saved as .csv file).')
    parser.add_argument('--graph_dir',
                        type=str,
                        default='./data',
                        help='The directory to save the constructed citation network.')

    args = parser.parse_args()
    return args


def get_combiner_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser('Create The Citation Network of European Court of Human Rights.')

    parser.add_argument('--judgment_graph',
                        type=str,
                        default='graphJUD',
                        help='The name to the file containing judgments graph.')
    parser.add_argument('--decision_graph',
                        type=str,
                        default='graphDEC',
                        help='The name to the file containing decision graph.')
    parser.add_argument('--graph',
                        type=str,
                        default='graph_JUDDEC',
                        help='The name to save the constructed graph.')
    parser.add_argument('--graphs_dir',
                        type=str,
                        default='./data',
                        help='The directory to read networks.')
    parser.add_argument('--graph_dir',
                        type=str,
                        default=None,
                        help='The directory to save the constructed citation network.')

    args = parser.parse_args()
    return args


def save_args(args: argparse.Namespace, directory_path: str) -> None:
    """
    Save the arguments in the specified directory as
        - a text file called 'args.txt'
        - a pickle file called 'args.pickle'
    :param args: The arguments to be saved
    :param directory_path: The path to the directory where the arguments should be saved
    """
    # If the specified directory does not exists, create it
    if not os.path.isdir(directory_path):
        os.mkdir(directory_path)
    # Save the args in a text file
    with open(directory_path + '/args.txt', 'w') as f:
        for arg in vars(args):
            val = getattr(args, arg)
            if isinstance(val, str):  # Add quotation marks to indicate that the argument is of string type
                val = f"'{val}'"
            f.write('{}: {}\n'.format(arg, val))
    # Pickle the args for possible reuse
    with open(directory_path + '/args.pickle', 'wb') as f:
        pickle.dump(args, f)


def load_args(directory_path: str) -> argparse.Namespace:
    """
    Load the pickled arguments from the specified directory
    :param directory_path: The path to the directory from which the arguments should be loaded
    :return: the unpickled arguments
    """
    with open(directory_path + '/args.pickle', 'rb') as f:
        args = pickle.load(f)
    return args



