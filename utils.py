from datetime import datetime


def parse_checkout(time_str):
    time_str = time_str.strip()  # Remove leading/trailing spaces

    # Parse time only
    return datetime.strptime(time_str, "%I %M %p").time()


def parse_checkin(time_str):
    time_str = time_str.strip()  # Remove leading/trailing spaces

    # Check if time is "Flexible"
    if time_str == "Flexible":
        # Impute value with the mode
        return datetime.strptime("15:00:00", "%H:%M:%S").time()

    # Extract the time part
    time_part = time_str.split()[-3:]
    time_str = ' '.join(time_part)

    # Parse time only
    return datetime.strptime(time_str, "%I %M %p").time()
