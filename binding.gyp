{
  "targets": [
    {
      "target_name": "epq",
      "sources": [
        "src/queue.cpp",
        "src/binding.cpp",
      ],
      'cflags':[
        '-O3',
      ],
      'include_dirs': [
        "inc",
        "<!(node -e \"require('nan')\")",
      ],
    },
  ],
}
